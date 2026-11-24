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
import math
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
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
NOAA_METAR_URL = "https://aviationweather.gov/api/data/metar"
NOAA_TAF_URL = "https://aviationweather.gov/api/data/taf"
ASTRO_SHEET_URL = "https://script.google.com/macros/s/AKfycbwuUSq37Tt2L7tXlUOumbFIZor40WbshBrhzgNYzc7FUDdL1vCOHxik3mwZtftKvhoq/exec"
INDONESIA_BBOX = "-11,95,6,141"
INDONESIA_ICAO_PREFIXES = ("WA", "WI", "WR")
INDONESIA_NOAA_EXTRA_ICAOS = (
    "WADD", "WADL", "WARR", "WAHI", "WAQQ", "WAMM", "WAPP", "WABB",
    "WASS", "WARQ", "WIHH", "WIII", "WALL", "WAAA", "WIMM", "WAHS",
    "WAJJ", "WAOO", "WATT", "WIBB", "WIDN", "WIOO", "WIPP", "WICC",
    "WICA", "WAGG", "WIDD", "WALS", "WAQT", "WAQC", "WITT", "WIEE",
    "WIGG", "WIKK", "WILL",
)
INDONESIA_AIRPORT_NAMES = {
    "WADD": "I Gusti Ngurah Rai Intl (Bali)",
    "WADL": "Zainuddin Abdul Madjid Intl (Lombok)",
    "WARR": "Juanda Intl (Surabaya)",
    "WAHI": "Yogyakarta Intl (YIA)",
    "WAQQ": "Juwata Intl (Tarakan)",
    "WAMM": "Sam Ratulangi Intl (Manado)",
    "WAPP": "Pattimura Airport (Ambon)",
    "WABB": "Frans Kaisiepo Airport (Biak)",
    "WASS": "Domine Eduard Osok Airport (Sorong)",
    "WARQ": "Adi Soemarmo Intl (Solo)",
    "WIHH": "Halim Perdanakusuma Intl (Jakarta)",
    "WIII": "Soekarno-Hatta Intl (Jakarta)",
    "WALL": "Sultan Aji Muhammad Sulaiman Sepinggan Intl (Balikpapan)",
    "WAAA": "Sultan Hasanuddin Intl (Makassar)",
    "WIMM": "Kualanamu Intl (Medan)",
}
USER_AGENT = "AWOSWeatherScraper/2.0 (+Python urllib)"
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
        "iata": None,
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
        "taf": None,
        "taf_issued_utc": None,
        "source": "AWOS",
        "fetched_at_utc": fetched_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fetched_at_wib": fetched_utc.astimezone(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00"),
    }


def compute_rh_pct(temp_c: float | None, dewp_c: float | None) -> int | None:
    """Hitung Relative Humidity (%) dari suhu dan dew point (rumus Magnus)."""
    if temp_c is None or dewp_c is None:
        return None
    try:
        es = math.exp((17.625 * temp_c) / (243.04 + temp_c))
        e = math.exp((17.625 * dewp_c) / (243.04 + dewp_c))
        rh = round(100.0 * (e / es))
        return max(0, min(100, int(rh)))
    except Exception:
        return None


def clean_noaa_airport_name(icao: str, raw_name: str | None) -> str:
    """Bersihkan nama bandara NOAA agar rapi dan sesuai standar aviasi."""
    if icao in INDONESIA_AIRPORT_NAMES:
        return INDONESIA_AIRPORT_NAMES[icao]
    text = clean_str(raw_name)
    if not text:
        return f"{icao} Aerodrome"
    # Hapus sufiks negara/provinsi seperti ', BA, ID' atau ', ID'
    text = re.sub(r",\s*[A-Z]{2},\s*ID$", "", text)
    text = re.sub(r",\s*ID$", "", text)
    return text.strip()


def extract_metric_vis_from_noaa(raw_ob: str | None, fallback_visib: Any) -> tuple[float | None, str | None]:
    """Ambil visibilitas metrik (meter) langsung dari METAR ICAO; fallback ke konversi SM -> meter."""
    if raw_ob:
        upper = raw_ob.upper()
        if re.search(r"\bCAVOK\b", upper):
            return 9999.0, "meters"
        # Cari grup visibilitas 4 digit tepat setelah grup angin (misal 18006KT 9999 atau 27005KT 170V240 5000)
        m = re.search(
            r"\b(?:VRB|\d{3})\d{2,3}(?:G\d{2,3})?KT(?:\s+\d{3}V\d{3})?\s+(\d{4})\b",
            upper,
        )
        if m:
            return float(int(m.group(1))), "meters"

    if fallback_visib is not None:
        s = str(fallback_visib).strip()
        if s.endswith("+"):
            return 9999.0, "meters"
        val = to_float(s)
        if val is not None:
            meters = min(9999.0, round(val * 1609.34))
            return meters, "meters"
    return None, None


def extract_clouds_from_noaa(item: dict[str, Any]) -> str | None:
    """Ekstrak lapisan awan ICAO (termasuk CB/TCU dari raw METAR jika tersedia)."""
    raw_ob = clean_str(item.get("rawOb")) or ""
    if raw_ob:
        tokens = re.findall(
            r"\b(?:FEW|SCT|BKN|OVC|VV)\d{3}(?:CB|TCU)?\b|\b(?:CAVOK|NSC|NCD|SKC|CLR)\b",
            raw_ob.upper(),
        )
        if tokens:
            # Hilangkan duplikat berurutan sambil menjaga urutan
            dedup: list[str] = []
            for tok in tokens:
                if tok not in dedup:
                    dedup.append(tok)
            return " ".join(dedup)

    clouds = item.get("clouds")
    if isinstance(clouds, list) and clouds:
        parts: list[str] = []
        for c in clouds:
            if not isinstance(c, dict):
                continue
            cov = clean_str(c.get("cover"))
            base = c.get("base")
            if cov and isinstance(base, (int, float)):
                parts.append(f"{cov.upper()}{int(round(base / 100.0)):03d}")
            elif cov:
                parts.append(cov.upper())
        if parts:
            return " ".join(parts)
    return None


def parse_noaa_metar_record(item: dict[str, Any], fetched_utc: datetime) -> dict[str, Any] | None:
    """Konversi 1 item JSON NOAA METAR menjadi format record standar AWOS."""
    icao = (clean_str(item.get("icaoId")) or "").upper()
    if len(icao) != 4:
        return None

    obs_epoch = to_int(str(item["obsTime"])) if item.get("obsTime") is not None else None
    if obs_epoch is not None:
        obs_dt = datetime.fromtimestamp(obs_epoch, tz=timezone.utc)
        observed_at_utc = obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        observed_at_wib = obs_dt.astimezone(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00")
        observed_date = obs_dt.strftime("%m/%d/%Y")
        observed_time_utc = obs_dt.strftime("%H:%M:%S")
    else:
        observed_at_utc = None
        observed_at_wib = None
        observed_date = None
        observed_time_utc = None

    raw_ob = clean_str(item.get("rawOb"))
    temp_c = to_float(str(item["temp"])) if item.get("temp") is not None else None
    dewp_c = to_float(str(item["dewp"])) if item.get("dewp") is not None else None
    rh_pct = compute_rh_pct(temp_c, dewp_c)

    raw_wdir = item.get("wdir")
    wind_dir: int | None = None
    wind_var: str | None = None
    if isinstance(raw_wdir, str) and raw_wdir.strip().upper() == "VRB":
        wind_var = "VRB"
    elif raw_wdir is not None:
        wind_dir = to_int(str(raw_wdir))

    wind_spd = to_int(str(item["wspd"])) if item.get("wspd") is not None else None
    wind_gst = to_int(str(item["wgst"])) if item.get("wgst") is not None else None

    altim = to_float(str(item["altim"])) if item.get("altim") is not None else None
    altim_unit = "hPa" if altim is not None else None

    vis_val, vis_unit = extract_metric_vis_from_noaa(raw_ob, item.get("visib"))
    sky_cond = extract_clouds_from_noaa(item)
    pwx = clean_str(item.get("wxString"))

    elev_m = to_float(str(item["elev"])) if item.get("elev") is not None else None
    elev_ft = int(round(elev_m * 3.28084)) if elev_m is not None else None

    lat = to_float(str(item["lat"])) if item.get("lat") is not None else None
    lon = to_float(str(item["lon"])) if item.get("lon") is not None else None
    if lat is not None:
        lat = round(lat, 6)
    if lon is not None:
        lon = round(lon, 6)

    airport_name = clean_noaa_airport_name(icao, item.get("name"))

    return {
        "icao": icao,
        "iata": None,
        "airport_name": airport_name,
        "directory_name": airport_name,
        "region": "Indonesia",
        "url": f"https://aviationweather.gov/data/metar/?ids={icao}&taf=1",
        "latitude": lat,
        "longitude": lon,
        "elevation_ft": elev_ft,
        "runway": None,
        "display_units": "METRIC",
        "observed_date": observed_date,
        "observed_time_utc": observed_time_utc,
        "observed_at_utc": observed_at_utc,
        "observed_at_wib": observed_at_wib,
        "observed_epoch": obs_epoch,
        "temperature_c": temp_c,
        "dew_point_c": dewp_c,
        "humidity_pct": rh_pct,
        "altimeter_value": altim,
        "altimeter_unit": altim_unit,
        "density_altitude_ft": None,
        "wind_speed_kt": wind_spd,
        "wind_direction_deg": wind_dir,
        "wind_gust_kt": wind_gst,
        "wind_variable": wind_var,
        "visibility_value": vis_val,
        "visibility_unit": vis_unit,
        "sky_condition": sky_cond,
        "present_weather": pwx,
        "rainfall_hour": None,
        "rainfall_unit": None,
        "day_night": None,
        "metar": raw_ob,
        "taf": None,
        "taf_issued_utc": None,
        "source": "NOAA",
        "fetched_at_utc": fetched_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fetched_at_wib": fetched_utc.astimezone(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00"),
    }


def _http_get_json_list(url: str, timeout: int) -> list[dict[str, Any]]:
    """Ambil list JSON dari NOAA Aviation Weather API (mengembalikan [] jika kosong/204)."""
    text = http_get_text(url, timeout=timeout, retries=2).strip()
    if not text:
        return []
    parsed = json.loads(text)
    if isinstance(parsed, list):
        return [x for x in parsed if isinstance(x, dict)]
    return []


def fetch_noaa_indonesia_metars(
    target_icaos: list[str], timeout: int, logger: logging.Logger
) -> dict[str, dict[str, Any]]:
    """Ambil live METAR Indonesia (+ alternate eksplisit) dari NOAA."""
    result: dict[str, dict[str, Any]] = {}
    explicit_set = {c.upper() for c in target_icaos}
    urls = [
        f"{NOAA_METAR_URL}?bbox={urllib.parse.quote(INDONESIA_BBOX)}&format=json",
    ]
    if target_icaos:
        # Pecah per 50 ICAO agar URL tetap pendek dan stabil
        for i in range(0, len(target_icaos), 50):
            chunk = ",".join(target_icaos[i : i + 50])
            urls.append(f"{NOAA_METAR_URL}?ids={urllib.parse.quote(chunk)}&format=json")

    for url in urls:
        try:
            items = _http_get_json_list(url, timeout=timeout)
            for item in items:
                code = (clean_str(item.get("icaoId")) or "").upper()
                if len(code) == 4 and (code.startswith(INDONESIA_ICAO_PREFIXES) or code in explicit_set):
                    prev = result.get(code)
                    if prev is None or (item.get("obsTime") or 0) >= (prev.get("obsTime") or 0):
                        result[code] = item
        except Exception as exc:
            logger.warning("NOAA METAR fetch warning (%s): %s", url.split("?")[1][:32], exc)
    return result


def fetch_noaa_indonesia_tafs(
    target_icaos: list[str], timeout: int, logger: logging.Logger
) -> dict[str, dict[str, Any]]:
    """Ambil live TAF/TAFOR Indonesia (+ alternate eksplisit) dari NOAA Aviation Weather API."""
    result: dict[str, dict[str, Any]] = {}
    explicit_set = {c.upper() for c in target_icaos}
    urls = [
        f"{NOAA_TAF_URL}?bbox={urllib.parse.quote(INDONESIA_BBOX)}&format=json",
    ]
    if target_icaos:
        for i in range(0, len(target_icaos), 50):
            chunk = ",".join(target_icaos[i : i + 50])
            urls.append(f"{NOAA_TAF_URL}?ids={urllib.parse.quote(chunk)}&format=json")

    for url in urls:
        try:
            items = _http_get_json_list(url, timeout=timeout)
            for item in items:
                code = (clean_str(item.get("icaoId")) or "").upper()
                raw_taf = clean_str(item.get("rawTAF"))
                if len(code) == 4 and (code.startswith(INDONESIA_ICAO_PREFIXES) or code in explicit_set) and raw_taf:
                    prev = result.get(code)
                    cur_issue = str(item.get("issueTime") or "")
                    prev_issue = str(prev.get("issueTime") or "") if prev else ""
                    if prev is None or cur_issue >= prev_issue:
                        result[code] = {
                            "raw_taf": raw_taf,
                            "issueTime": clean_str(item.get("issueTime")),
                        }
        except Exception as exc:
            logger.warning("NOAA TAF fetch warning (%s): %s", url.split("?")[1][:32], exc)
    return result


def fetch_astro_indonesia_airports(
    timeout: int, logger: logging.Logger
) -> dict[str, dict[str, Any]]:
    """Ambil daftar bandara (ICAO, IATA, Nama, Lat, Lon) dari Google Sheet Astro DB."""
    result: dict[str, dict[str, Any]] = {}
    try:
        text = http_get_text(ASTRO_SHEET_URL, timeout=max(timeout, 20), retries=2).strip()
        if not text:
            return result
        parsed = json.loads(text)
        rows = parsed.get("data") if isinstance(parsed, dict) else None
        if not isinstance(rows, list):
            return result
        for row in rows:
            if not isinstance(row, dict):
                continue
            icao = (clean_str(str(row.get("icao") or "")) or "").upper()
            if len(icao) != 4 or not icao.isalpha():
                continue
            if icao in result:
                continue  # Strict zero-duplicate guard
            iata = (clean_str(str(row.get("iata") or "")) or "").upper() or None
            name = clean_str(str(row.get("name") or "")) or f"{icao} Aerodrome"
            lat = to_float(str(row.get("lat")) if row.get("lat") is not None else None)
            lon = to_float(str(row.get("lon")) if row.get("lon") is not None else None)
            if lat is not None:
                lat = round(lat, 6)
            if lon is not None:
                lon = round(lon, 6)
            result[icao] = {
                "icao": icao,
                "iata": iata,
                "name": name,
                "lat": lat,
                "lon": lon,
            }
    except Exception as exc:
        logger.warning("Astro DB fetch warning (non-fatal): %s", exc)
    return result


def build_astro_only_record(entry: dict[str, Any], fetched_utc: datetime) -> dict[str, Any]:
    """Buat record standar untuk bandara Astro-only (tanpa sensor AWOS/NOAA live)."""
    icao = entry["icao"]
    iata = entry.get("iata")
    name = entry["name"]
    lat = entry.get("lat")
    lon = entry.get("lon")
    url = (
        f"https://skyvector.com/?ll={lat},{lon}&chart=302&zoom=3"
        if (lat is not None and lon is not None)
        else "https://skyvector.com/"
    )
    return {
        "icao": icao,
        "iata": iata,
        "airport_name": name,
        "directory_name": name,
        "region": "Indonesia",
        "url": url,
        "latitude": lat,
        "longitude": lon,
        "elevation_ft": None,
        "runway": None,
        "display_units": "METRIC",
        "observed_date": None,
        "observed_time_utc": None,
        "observed_at_utc": None,
        "observed_at_wib": None,
        "observed_epoch": None,
        "temperature_c": None,
        "dew_point_c": None,
        "humidity_pct": None,
        "altimeter_value": None,
        "altimeter_unit": None,
        "density_altitude_ft": None,
        "wind_speed_kt": None,
        "wind_direction_deg": None,
        "wind_gust_kt": None,
        "wind_variable": None,
        "visibility_value": None,
        "visibility_unit": None,
        "sky_condition": None,
        "present_weather": None,
        "rainfall_hour": None,
        "rainfall_unit": None,
        "day_night": None,
        "metar": None,
        "taf": None,
        "taf_issued_utc": None,
        "source": "ASTRO",
        "fetched_at_utc": fetched_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fetched_at_wib": fetched_utc.astimezone(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00"),
    }


def merge_hybrid_noaa_data(
    records: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    region: str | None,
    airports_filter: list[str] | None,
    timeout: int,
    fetched_utc: datetime,
    logger: logging.Logger,
    include_astro: bool = True,
) -> tuple[int, int, int, int]:
    """
    Gabungkan data AWOS Net + NOAA METAR/TAF + Astro DB (strict zero-duplicate by ICAO):
      1. Enrich (lengkapi) field kosong pada stasiun AWOS (misal METAR/QNH/VIS untuk WIHH).
      2. Tambah bandara utama Indonesia dari NOAA yang tidak ada di AWOS Net (WARR, WADD, WADL, WAHI, WAQQ, dll.).
      3. Tempelkan live TAF/TAFOR ke setiap bandara yang tersedia.
      4. Tempel kode IATA dari Astro DB ke bandara yang cocok & tambahkan bandara Astro-only (WIDM/MWK, WAQA/NNX, dll.) tanpa duplikat.
    Mengembalikan (noaa_added_count, noaa_enriched_count, taf_attached_count, astro_added_count).
    """
    is_indonesia_scope = bool(
        airports_filter
        or (region and region.strip().lower() == "indonesia")
    )
    if not is_indonesia_scope:
        return 0, 0, 0, 0

    allowed_set = {c.strip().upper() for c in airports_filter} if airports_filter else None

    existing_icaos = {r["icao"].upper(): r for r in records}
    failed_icaos = {f["icao"].upper() for f in failures if f.get("icao")}

    astro_map = fetch_astro_indonesia_airports(timeout=timeout, logger=logger) if include_astro else {}

    hint_set = (
        set(INDONESIA_NOAA_EXTRA_ICAOS)
        | set(existing_icaos.keys())
        | failed_icaos
        | set(astro_map.keys())
    )
    if allowed_set:
        hint_set = {c for c in allowed_set if c.startswith(INDONESIA_ICAO_PREFIXES)}

    target_icaos = sorted(hint_set)
    noaa_metars = fetch_noaa_indonesia_metars(target_icaos, timeout=timeout, logger=logger)
    noaa_tafs = fetch_noaa_indonesia_tafs(target_icaos, timeout=timeout, logger=logger)

    noaa_added = 0
    noaa_enriched = 0

    enrich_fields = (
        "metar",
        "altimeter_value",
        "visibility_value",
        "sky_condition",
        "present_weather",
        "temperature_c",
        "dew_point_c",
        "humidity_pct",
        "wind_speed_kt",
        "wind_direction_deg",
        "wind_gust_kt",
        "latitude",
        "longitude",
        "elevation_ft",
    )

    for code, raw_item in noaa_metars.items():
        if allowed_set and code not in allowed_set:
            continue
        noaa_rec = parse_noaa_metar_record(raw_item, fetched_utc)
        if not noaa_rec:
            continue

        if code in existing_icaos:
            awos_rec = existing_icaos[code]
            enriched_any = False
            for fld in enrich_fields:
                if awos_rec.get(fld) is None and noaa_rec.get(fld) is not None:
                    awos_rec[fld] = noaa_rec[fld]
                    enriched_any = True
                    if fld == "altimeter_value" and not awos_rec.get("altimeter_unit"):
                        awos_rec["altimeter_unit"] = noaa_rec.get("altimeter_unit")
                    elif fld == "visibility_value" and not awos_rec.get("visibility_unit"):
                        awos_rec["visibility_unit"] = noaa_rec.get("visibility_unit")
            if enriched_any:
                awos_rec["source"] = "AWOS+NOAA"
                noaa_enriched += 1
        else:
            records.append(noaa_rec)
            existing_icaos[code] = noaa_rec
            noaa_added += 1
            failures[:] = [f for f in failures if str(f.get("icao", "")).upper() != code]

    # Attach IATA & missing coordinates from Astro DB to live stations; append remaining Astro-only stations cleanly (zero duplicates)
    astro_added = 0
    for code, a_entry in astro_map.items():
        if allowed_set and code not in allowed_set:
            continue
        if code in existing_icaos:
            rec = existing_icaos[code]
            if a_entry.get("iata") and not rec.get("iata"):
                rec["iata"] = a_entry["iata"]
            if rec.get("latitude") is None and a_entry.get("lat") is not None:
                rec["latitude"] = a_entry["lat"]
            if rec.get("longitude") is None and a_entry.get("lon") is not None:
                rec["longitude"] = a_entry["lon"]
        else:
            astro_rec = build_astro_only_record(a_entry, fetched_utc)
            records.append(astro_rec)
            existing_icaos[code] = astro_rec
            astro_added += 1

    taf_attached = 0
    for code, taf_info in noaa_tafs.items():
        rec = existing_icaos.get(code)
        if rec is not None and taf_info.get("raw_taf"):
            rec["taf"] = taf_info["raw_taf"]
            rec["taf_issued_utc"] = taf_info.get("issueTime")
            taf_attached += 1

    return noaa_added, noaa_enriched, taf_attached, astro_added


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

    # Safe forward migration for existing SQLite databases
    airport_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(airports)").fetchall()
    }
    if "iata" not in airport_cols:
        conn.execute("ALTER TABLE airports ADD COLUMN iata TEXT")

    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(observations)").fetchall()
    }
    for col_name, col_type in (
        ("taf", "TEXT"),
        ("taf_issued_utc", "TEXT"),
        ("source", "TEXT"),
    ):
        if col_name not in existing_cols:
            conn.execute(f"ALTER TABLE observations ADD COLUMN {col_name} {col_type}")

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
                        icao, iata, airport_name, directory_name, region, url,
                        latitude, longitude, elevation_ft, runway, display_units, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(icao) DO UPDATE SET
                        iata = COALESCE(excluded.iata, airports.iata),
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
                        rec.get("iata"),
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

                # Jangan masukkan baris ASTRO-only kosong ke tabel riwayat observasi cuaca
                if rec.get("source") == "ASTRO" and not rec.get("observed_at_utc"):
                    continue

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
                        taf, taf_issued_utc, source,
                        fetched_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        rec.get("taf"),
                        rec.get("taf_issued_utc"),
                        rec.get("source", "AWOS"),
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
    if not stations and not (args.airports and not getattr(args, "no_noaa", False)):
        logger.warning("Tidak ada bandara yang cocok dengan filter '%s'.", region_label)
        return 1

    logger.info(
        "Ditemukan %d bandara AWOS Net. Mengambil data XML dengan %d worker paralel...",
        len(stations),
        args.workers,
    )

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    if stations:
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

    noaa_added = 0
    noaa_enriched = 0
    taf_attached = 0
    astro_added = 0
    if not getattr(args, "no_noaa", False):
        noaa_added, noaa_enriched, taf_attached, astro_added = merge_hybrid_noaa_data(
            records=records,
            failures=failures,
            region=region,
            airports_filter=args.airports,
            timeout=args.timeout,
            fetched_utc=started_utc,
            logger=logger,
            include_astro=True,
        )
        if noaa_added or noaa_enriched or taf_attached or astro_added:
            logger.info(
                "Hybrid NOAA+Astro: +%d NOAA | %d AWOS dilengkapi METAR | %d live TAF | +%d Astro-only",
                noaa_added,
                noaa_enriched,
                taf_attached,
                astro_added,
            )

    # Final defensive deduplication by ICAO
    dedup_records: dict[str, dict[str, Any]] = {}
    for r in records:
        code = str(r.get("icao", "")).upper()
        if code and code not in dedup_records:
            dedup_records[code] = r
    records = sorted(dedup_records.values(), key=lambda r: r["icao"])
    failures.sort(key=lambda r: r["icao"])

    live_wx_count = sum(1 for r in records if r.get("source") != "ASTRO")
    astro_only_count = sum(1 for r in records if r.get("source") == "ASTRO")

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
            "sources": ["AWOS Net", "NOAA METAR/TAF", "Astro DB"] if not getattr(args, "no_noaa", False) else ["AWOS Net"],
            "filter": region_label,
            "fetched_at_utc": finished_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fetched_at_wib": finished_utc.astimezone(WIB).strftime("%Y-%m-%dT%H:%M:%S+07:00"),
            "duration_seconds": duration_sec,
            "total_airports": len(records) + len(failures),
            "success_count": len(records),
            "live_wx_count": live_wx_count,
            "astro_only_count": astro_only_count,
            "noaa_added_count": noaa_added,
            "noaa_enriched_count": noaa_enriched,
            "taf_count": taf_attached,
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
        len(records) + len(failures),
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
        "--no-noaa",
        action="store_true",
        help="Nonaktifkan sumber hybrid NOAA METAR/TAF (hanya AWOS Net saja)",
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
