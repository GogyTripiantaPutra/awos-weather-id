#!/usr/bin/env python3
"""
AWOS Net Airport Weather Scraper (Indonesia & Global)
=====================================================
Mengambil data cuaca bandara secara otomatis dari http://awosnet.com/default.php
lalu menyimpannya ke:
  1. Database SQLite (riwayat historis lengkap, otomatis cegah duplikat)
  2. File JSON (snapshot data terbaru yang siap dibaca aplikasi lain)

Alur kerja scraping (tanpa library eksternal, cukup Python standar):
  1. Ambil halaman direktori HTML (http://awosnet.com/default.php) & ekstrak daftar link bandara.
  2. Untuk setiap bandara, ambil endpoint XML mentah (/awiAwosNet.php) secara paralel.
  3. Parsing XML menjadi struktur data bersih (angka, koordinat, waktu UTC & WIB, METAR).
  4. Simpan ke SQLite + tulis file JSON terbaru secara atomik.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

# Konstanta default
DIRECTORY_URL = "http://awosnet.com/default.php"
USER_AGENT = "AWOSWeatherScraper/1.0 (+Python urllib)"
WIB = timezone(timedelta(hours=7))

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "data" / "awos_weather.db"
DEFAULT_JSON_PATH = BASE_DIR / "data" / "latest_indonesia.json"
DEFAULT_SNAPSHOT_DIR = BASE_DIR / "data" / "snapshots"
DEFAULT_LOG_PATH = BASE_DIR / "logs" / "scraper.log"

# Nilai kosong / sensor error yang sering muncul pada sistem AWOS
MISSING_TOKENS = {"", "M", "NA", "N/A", "NULL", "NONE"}


class DirectoryLinkParser(HTMLParser):
    """Parser HTML sederhana untuk mengambil semua tag <a href="...">Nama</a>."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._current_href: str | None = None
        self._current_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            attr_map = dict(attrs)
            href = attr_map.get("href")
            if href:
                self._current_href = href.strip()
                self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._current_href is not None:
            text = "".join(self._current_text).strip()
            if text:
                self.links.append((self._current_href, text))
            self._current_href = None
            self._current_text = []


def setup_logging(log_path: Path, verbose: bool = False) -> logging.Logger:
    """Konfigurasi logging ke terminal dan file log berotasi."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("awos_scraper")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        log_path, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if sys.stdout is not None:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def http_get_text(url: str, timeout: int = 15, retries: int = 2) -> str:
    """Ambil isi teks dari sebuah URL dengan retry otomatis untuk error sementara."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return raw.decode("utf-8", errors="replace")
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(0.8 * attempt)
    assert last_err is not None
    raise last_err


def clean_str(val: str | None) -> str | None:
    """Bersihkan string kosong atau placeholder sensor (////, \\\\, M)."""
    if val is None:
        return None
    text = val.strip()
    if not text or text.upper() in MISSING_TOKENS:
        return None
    # Hapus placeholder seperti '///', '////', '\\\\'
    if set(text) <= {"/", "\\", "-", " "}:
        return None
    return text


def to_float(val: str | None) -> float | None:
    """Konversi string menjadi float dengan aman."""
    text = clean_str(val)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_int(val: str | None) -> int | None:
    """Konversi string menjadi int dengan aman (mendukung desimal seperti '12.0')."""
    num = to_float(val)
    if num is None:
        return None
    return int(round(num))


def normalize_coordinate(raw_val: str | None, is_longitude: bool = False) -> float | None:
    """
    Normalisasi koordinat GPS.
    Beberapa stasiun AWOS mengirim format NMEA (DDMM.MMMM / DDDMM.MMMM, misal '10406.8153')
    alih-alih derajat desimal ('104.1136'). Fungsi ini otomatis mengonversinya.
    """
    val = to_float(raw_val)
    if val is None:
        return None

    limit = 180.0 if is_longitude else 90.0
    if abs(val) <= limit:
        return round(val, 6)

    # Konversi NMEA (DDMM.MMMM atau DDDMM.MMMM) -> derajat desimal
    sign = -1.0 if val < 0 else 1.0
    abs_val = abs(val)
    degrees = int(abs_val // 100)
    minutes = abs_val - (degrees * 100)
    if minutes >= 60.0:
        return None
    decimal = sign * (degrees + minutes / 60.0)
    if abs(decimal) > limit:
        return None
    return round(decimal, 6)


def parse_observed_datetime(
    date_str: str | None, time_str: str | None, epoch_str: str | None
) -> tuple[str | None, str | None, int | None]:
    """
    Ubah tanggal & waktu AWOS (UTC) menjadi format ISO-8601 UTC, ISO-8601 WIB, dan epoch.
    Format tanggal di awosnet bisa DD/MM/YYYY (internasional) atau MM/DD/YYYY (US).
    """
    epoch_val: int | None = None
    if epoch_str and epoch_str.isdigit():
        epoch_val = int(epoch_str)

    dt_utc: datetime | None = None
    if date_str and time_str:
        # Coba format DD/MM/YYYY terlebih dahulu (dipakai bandara Indonesia)
        for fmt in ("%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt_utc = datetime.strptime(f"{date_str} {time_str}", fmt).replace(
                    tzinfo=timezone.utc
                )
                break
            except ValueError:
                continue

    if dt_utc is None and epoch_val is not None:
        dt_utc = datetime.fromtimestamp(epoch_val, tz=timezone.utc)

    if dt_utc is None:
        return None, None, epoch_val

    if epoch_val is None:
        epoch_val = int(dt_utc.timestamp())

    iso_utc = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    iso_wib = dt_utc.astimezone(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00")
    return iso_utc, iso_wib, epoch_val


def infer_icao_from_url(url: str) -> str:
    """Ambil perkiraan kode ICAO dari subdomain (misal http://wirr.awosnet.com -> WIRR)."""
    match = re.match(r"https?://([^./]+)\.", url.lower())
    return match.group(1).upper() if match else ""


def fetch_airport_directory(
    region: str | None = "Indonesia",
    airports_filter: list[str] | None = None,
    timeout: int = 15,
) -> list[dict[str, str]]:
    """
    Tahap 1: Ambil daftar bandara dari halaman utama http://awosnet.com/default.php.
    """
    html = http_get_text(DIRECTORY_URL, timeout=timeout)
    parser = DirectoryLinkParser()
    parser.feed(html)

    allowed_codes = {code.strip().upper() for code in (airports_filter or []) if code.strip()}
    region_lower = region.strip().lower() if region else None

    results: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for href, label in parser.links:
        if not href.startswith("http"):
            continue
        clean_url = href.rstrip("/")
        if clean_url in seen_urls:
            continue

        # Format label di situs: "Indonesia - Soekarno Hatta Airport" atau "CA - Auburn Municipal"
        if " - " in label:
            dir_region, dir_name = label.split(" - ", 1)
        else:
            dir_region, dir_name = "", label

        dir_region = dir_region.strip()
        dir_name = dir_name.strip()
        icao_guess = infer_icao_from_url(clean_url)

        if allowed_codes:
            if icao_guess not in allowed_codes:
                continue
        elif region_lower:
            if dir_region.lower() != region_lower:
                continue

        seen_urls.add(clean_url)
        results.append(
            {
                "icao_guess": icao_guess,
                "region": dir_region,
                "directory_name": dir_name,
                "url": clean_url,
            }
        )

    return results


def parse_awos_xml(xml_text: str, station_meta: dict[str, str], fetched_utc: datetime) -> dict[str, Any]:
    """
    Tahap 3: Parsing XML dari endpoint /awiAwosNet.php menjadi dictionary terstruktur.
    """
    if "Error code: 404" in xml_text or xml_text.lstrip().startswith("<!DOCTYPE"):
        raise ValueError("Feed AWOS tidak tersedia / offline (404)")

    root = ET.fromstring(xml_text)

    def get_val(*tags: str) -> str | None:
        for tag in tags:
            el = root.find(tag)
            if el is not None:
                val = clean_str(el.attrib.get("value"))
                if val is not None:
                    return val
        return None

    def get_attr(tag_candidates: tuple[str, ...], attr: str) -> str | None:
        for tag in tag_candidates:
            el = root.find(tag)
            if el is not None:
                val = clean_str(el.attrib.get(attr))
                if val is not None:
                    return val
        return None

    icao = (
        get_val("airportIdentifier", "airportID", "wmscrId")
        or station_meta["icao_guess"]
        or "UNKNOWN"
    ).upper()

    date_str = get_val("date")
    time_str = get_val("time")
    epoch_str = get_attr(("time", "date", "airTemperature", "altimeterSetting"), "time")
    observed_at_utc, observed_at_wib, observed_epoch = parse_observed_datetime(
        date_str, time_str, epoch_str
    )

    vis_val = to_float(get_val("tenMinuteVisibility", "tenMinutevisibility"))
    vis_unit = get_attr(("tenMinuteVisibility", "tenMinutevisibility"), "units")
    if vis_val is not None:
        vis_val = round(vis_val, 1)

    rain_val = to_float(get_val("rainfallCurrentHour"))
    if rain_val is not None:
        rain_val = round(rain_val, 3)

    altimeter_val = to_float(get_val("altimeterSetting"))
    altimeter_unit = get_attr(("altimeterSetting",), "units")

    return {
        "icao": icao,
        "airport_name": get_val("airportName") or station_meta["directory_name"],
        "directory_name": station_meta["directory_name"],
        "region": station_meta["region"],
        "url": station_meta["url"],
        "latitude": normalize_coordinate(get_val("siteLatitude"), is_longitude=False),
        "longitude": normalize_coordinate(get_val("siteLongitude"), is_longitude=True),
        "elevation_ft": to_int(get_val("siteElevation")),
        "runway": get_val("runwayA"),
        "display_units": get_val("displayUnits"),
        "observed_date": date_str,
        "observed_time_utc": time_str,
        "observed_at_utc": observed_at_utc,
        "observed_at_wib": observed_at_wib,
        "observed_epoch": observed_epoch,
        "temperature_c": to_float(get_val("airTemperature")),
        "dew_point_c": to_float(get_val("dewPoint")),
        "humidity_pct": to_int(get_val("relativeHumidity")),
        "altimeter_value": altimeter_val,
        "altimeter_unit": altimeter_unit,
        "density_altitude_ft": to_int(get_val("densityAltitude")),
        "wind_speed_kt": to_int(get_val("twoMinuteWindSpeed", "twoMinutewindSpeed", "windSpeed")),
        "wind_direction_deg": to_int(
            get_val("twoMinuteWindDirection", "twoMinutewindDirection", "windDirection")
        ),
        "wind_gust_kt": to_int(get_val("windGust")),
        "wind_variable": get_val("variableWinds", "windDirectionVariableString"),
        "visibility_value": vis_val,
        "visibility_unit": vis_unit,
        "sky_condition": get_val("SkyCondition", "ceilometer"),
        "present_weather": get_val("presentWeather"),
        "rainfall_hour": rain_val,
        "rainfall_unit": get_attr(("rainfallCurrentHour",), "units"),
        "day_night": get_val("dayNight"),
        "metar": get_val("METAR"),
        "fetched_at_utc": fetched_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fetched_at_wib": fetched_utc.astimezone(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00"),
    }


def fetch_one_airport(
    station: dict[str, str], timeout: int, fetched_utc: datetime
) -> tuple[bool, dict[str, Any]]:
    """Tahap 2: Ambil & parse data XML untuk 1 bandara."""
    xml_url = f"{station['url']}/awiAwosNet.php"
    try:
        xml_text = http_get_text(xml_url, timeout=timeout, retries=2)
        record = parse_awos_xml(xml_text, station, fetched_utc)
        return True, record
    except Exception as exc:
        return False, {
            "icao": station["icao_guess"],
            "directory_name": station["directory_name"],
            "region": station["region"],
            "url": station["url"],
            "error": f"{type(exc).__name__}: {exc}",
        }


def init_database(db_path: Path) -> sqlite3.Connection:
    """Siapkan file SQLite dan skema tabel jika belum ada."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS airports (
            icao TEXT PRIMARY KEY,
            airport_name TEXT,
            directory_name TEXT,
            region TEXT,
            url TEXT,
            latitude REAL,
            longitude REAL,
            elevation_ft INTEGER,
            runway TEXT,
            display_units TEXT,
            updated_at_utc TEXT
        );

        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            icao TEXT NOT NULL,
            observed_at_utc TEXT NOT NULL,
            observed_at_wib TEXT,
            observed_epoch INTEGER,
            temperature_c REAL,
            dew_point_c REAL,
            humidity_pct INTEGER,
            altimeter_value REAL,
            altimeter_unit TEXT,
            density_altitude_ft INTEGER,
            wind_speed_kt INTEGER,
            wind_direction_deg INTEGER,
            wind_gust_kt INTEGER,
            wind_variable TEXT,
            visibility_value REAL,
            visibility_unit TEXT,
            sky_condition TEXT,
            present_weather TEXT,
            rainfall_hour REAL,
            rainfall_unit TEXT,
            day_night TEXT,
            metar TEXT,
            fetched_at_utc TEXT NOT NULL,
            UNIQUE(icao, observed_at_utc),
            FOREIGN KEY (icao) REFERENCES airports(icao)
        );

        CREATE INDEX IF NOT EXISTS idx_obs_icao_time
            ON observations(icao, observed_at_utc DESC);

        CREATE INDEX IF NOT EXISTS idx_obs_fetched
            ON observations(fetched_at_utc DESC);

        CREATE TABLE IF NOT EXISTS fetch_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT NOT NULL,
            region_filter TEXT,
            total_airports INTEGER NOT NULL,
            success_count INTEGER NOT NULL,
            new_observations INTEGER NOT NULL,
            failed_count INTEGER NOT NULL,
            failed_details_json TEXT
        );
        """
    )
    conn.commit()
    return conn


def save_to_sqlite(
    db_path: Path,
    records: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    started_utc: datetime,
    finished_utc: datetime,
    region_label: str,
) -> int:
    """
    Tahap 4A: Simpan daftar bandara, data observasi cuaca (anti-duplikat), dan log run ke SQLite.
    Mengembalikan jumlah baris observasi baru yang benar-benar masuk.
    """
    conn = init_database(db_path)
    new_obs_count = 0

    try:
        with conn:
            for rec in records:
                conn.execute(
                    """
                    INSERT INTO airports (
                        icao, airport_name, directory_name, region, url,
                        latitude, longitude, elevation_ft, runway, display_units, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(icao) DO UPDATE SET
                        airport_name = excluded.airport_name,
                        directory_name = excluded.directory_name,
                        region = excluded.region,
                        url = excluded.url,
                        latitude = COALESCE(excluded.latitude, airports.latitude),
                        longitude = COALESCE(excluded.longitude, airports.longitude),
                        elevation_ft = COALESCE(excluded.elevation_ft, airports.elevation_ft),
                        runway = COALESCE(excluded.runway, airports.runway),
                        display_units = COALESCE(excluded.display_units, airports.display_units),
                        updated_at_utc = excluded.updated_at_utc
                    """,
                    (
                        rec["icao"],
                        rec["airport_name"],
                        rec["directory_name"],
                        rec["region"],
                        rec["url"],
                        rec["latitude"],
                        rec["longitude"],
                        rec["elevation_ft"],
                        rec["runway"],
                        rec["display_units"],
                        rec["fetched_at_utc"],
                    ),
                )

                # Gunakan observed_at_utc jika tersedia; jika tidak, fallback ke fetched_at_utc
                obs_key = rec["observed_at_utc"] or rec["fetched_at_utc"]
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO observations (
                        icao, observed_at_utc, observed_at_wib, observed_epoch,
                        temperature_c, dew_point_c, humidity_pct,
                        altimeter_value, altimeter_unit, density_altitude_ft,
                        wind_speed_kt, wind_direction_deg, wind_gust_kt, wind_variable,
                        visibility_value, visibility_unit,
                        sky_condition, present_weather,
                        rainfall_hour, rainfall_unit, day_night, metar,
                        fetched_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rec["icao"],
                        obs_key,
                        rec["observed_at_wib"],
                        rec["observed_epoch"],
                        rec["temperature_c"],
                        rec["dew_point_c"],
                        rec["humidity_pct"],
                        rec["altimeter_value"],
                        rec["altimeter_unit"],
                        rec["density_altitude_ft"],
                        rec["wind_speed_kt"],
                        rec["wind_direction_deg"],
                        rec["wind_gust_kt"],
                        rec["wind_variable"],
                        rec["visibility_value"],
                        rec["visibility_unit"],
                        rec["sky_condition"],
                        rec["present_weather"],
                        rec["rainfall_hour"],
                        rec["rainfall_unit"],
                        rec["day_night"],
                        rec["metar"],
                        rec["fetched_at_utc"],
                    ),
                )
                if cur.rowcount == 1:
                    new_obs_count += 1

            conn.execute(
                """
                INSERT INTO fetch_runs (
                    started_at_utc, finished_at_utc, region_filter,
                    total_airports, success_count, new_observations,
                    failed_count, failed_details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    started_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    finished_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    region_label,
                    len(records) + len(failures),
                    len(records),
                    new_obs_count,
                    len(failures),
                    json.dumps(failures, ensure_ascii=False),
                ),
            )
    finally:
        conn.close()

    return new_obs_count


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Tulis file JSON secara atomik (lewat file .tmp lalu replace) agar tidak korup saat dibaca."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def run_once(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Jalankan satu siklus scraping penuh."""
    started_utc = datetime.now(timezone.utc)
    region = None if args.all else args.region
    region_label = (
        f"airports:{','.join(args.airports)}"
        if args.airports
        else ("ALL" if args.all else (region or "ALL"))
    )

    logger.info("Memulai scraping AWOS Net (filter=%s)...", region_label)

    stations = fetch_airport_directory(
        region=region, airports_filter=args.airports, timeout=args.timeout
    )
    if not stations:
        logger.warning("Tidak ada bandara yang cocok dengan filter '%s'.", region_label)
        return 1

    logger.info(
        "Ditemukan %d bandara. Mengambil data XML dengan %d worker paralel...",
        len(stations),
        args.workers,
    )

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(fetch_one_airport, st, args.timeout, started_utc)
            for st in stations
        ]
        for fut in as_completed(futures):
            ok, data = fut.result()
            if ok:
                records.append(data)
            else:
                failures.append(data)

    records.sort(key=lambda r: r["icao"])
    failures.sort(key=lambda r: r["icao"])

    finished_utc = datetime.now(timezone.utc)
    duration_sec = round((finished_utc - started_utc).total_seconds(), 2)

    if args.json_only:
        new_obs = 0
    else:
        new_obs = save_to_sqlite(
            db_path=Path(args.db),
            records=records,
            failures=failures,
            started_utc=started_utc,
            finished_utc=finished_utc,
            region_label=region_label,
        )

    payload = {
        "meta": {
            "source": DIRECTORY_URL,
            "filter": region_label,
            "fetched_at_utc": finished_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fetched_at_wib": finished_utc.astimezone(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00"),
            "duration_seconds": duration_sec,
            "total_airports": len(stations),
            "success_count": len(records),
            "new_observations_saved": new_obs,
            "failed_count": len(failures),
        },
        "airports": records,
        "failed": failures,
    }

    json_path = Path(args.json)
    write_json_atomic(json_path, payload)

    if args.snapshot:
        snap_name = f"awos_{finished_utc.astimezone(WIB).strftime('%Y%m%d_%H%M%S')}.json"
        snap_path = DEFAULT_SNAPSHOT_DIR / snap_name
        write_json_atomic(snap_path, payload)
        logger.info("Snapshot JSON disimpan ke: %s", snap_path)

    logger.info(
        "Selesai dalam %.2f detik | Sukses: %d/%d | Observasi baru di SQLite: %d | Gagal/Offline: %d",
        duration_sec,
        len(records),
        len(stations),
        new_obs,
        len(failures),
    )

    if failures:
        failed_summary = ", ".join(f"{f['icao']} ({f['directory_name']})" for f in failures[:5])
        if len(failures) > 5:
            failed_summary += f" (+{len(failures) - 5} lainnya)"
        logger.info("Bandara offline/gagal: %s", failed_summary)

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Script otomatis pengambil data cuaca bandara dari http://awosnet.com/default.php"
    )
    parser.add_argument(
        "--region",
        default="Indonesia",
        help="Filter wilayah di awosnet.com (default: Indonesia)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Ambil semua 531 bandara di awosnet.com (mengabaikan --region)",
    )
    parser.add_argument(
        "--airports",
        nargs="+",
        help="Ambil kode ICAO tertentu saja, misal: --airports WIII WICC WIDD",
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help=f"Lokasi file database SQLite (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--json",
        default=str(DEFAULT_JSON_PATH),
        help=f"Lokasi file JSON terbaru (default: {DEFAULT_JSON_PATH})",
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Hanya tulis file JSON tanpa menyimpan ke SQLite (cocok untuk GitHub Actions/cloud)",
    )
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Selain menimpa JSON terbaru, simpan juga salinan arsip ber-timestamp di data/snapshots/",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Jumlah request paralel (default: 10)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="Batas waktu request HTTP dalam detik (default: 15)",
    )
    parser.add_argument(
        "--loop",
        type=int,
        default=0,
        help="Opsional: jalankan terus-menerus tiap N menit di terminal (misal --loop 30)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Tampilkan log detail (DEBUG)",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    logger = setup_logging(DEFAULT_LOG_PATH, verbose=args.verbose)

    if args.loop > 0:
        logger.info("Mode loop aktif: mengambil data setiap %d menit (Ctrl+C untuk berhenti).", args.loop)
        try:
            while True:
                try:
                    run_once(args, logger)
                except Exception:
                    logger.exception("Terjadi error saat mengambil data pada siklus ini.")
                logger.info("Menunggu %d menit untuk siklus berikutnya...", args.loop)
                time.sleep(args.loop * 60)
        except KeyboardInterrupt:
            logger.info("Dihentikan oleh pengguna.")
            return 0
    else:
        try:
            return run_once(args, logger)
        except Exception:
            logger.exception("Fatal error saat menjalankan scraper.")
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
