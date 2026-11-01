"""Planetary Anomaly & Threat Report — Background Worker Engine.

This module provides a production-grade, local-first automated daemon that
synthesizes planetary telemetry, space weather, seismic anomalies, oceanic
disturbances, and orbital launches into a publication-quality daily intelligence
briefing dispatched via encrypted SMTP.

Architectural Constraints & Engineering Rigor:
    * Zero Disk I/O: Designed specifically for constrained host environments
      (e.g., flash memory wear limits, strictly partitioned edge nodes, ephemeral
      containers, or severely restricted disk quotas). Zero bytes are written to
      disk during telemetry acquisition, JSON/matrix deserialization, image
      transformations, or PDF generation. All operations occur strictly within
      volatile memory buffers (`io.BytesIO`).
    * In-Memory PDF Assembly: ReportLab Platypus flowables (`SimpleDocTemplate`,
      `KeepTogether`) are deliberately avoided due to unpredictable reflow and
      `LayoutError` exceptions under variable input payloads. Instead, a
      pixel-exact, single-page vector dashboard is rendered using absolute X/Y
      coordinates via `reportlab.pdfgen.canvas`.
    * Concurrent Async Ingestion: Asynchronous parallel HTTP execution across
      all telemetry feeds via `httpx.AsyncClient` and `asyncio.gather`, coupled
      with exponential backoff retries via `tenacity` to absorb upstream 429/502s.
    * High-Performance Parsing: Ingestion payloads are parsed through Pydantic V2
      models utilizing its underlying Rust-based parsing engine and `AliasPath`
      expressions for zero-allocation selective field projection.
    * Secure Cryptographic Dispatch: Reports are transmitted over STARTTLS
      authenticated SMTP with certificate trust verification grounded in the
      host operating system's root certificate store.

Dependencies:
    `httpx`, `pydantic>=2.0`, `pydantic-settings`, `reportlab`, `tenacity`, `Pillow`.

Execution:
    python worker.py
    (Scheduled via Windows Task Scheduler or cron using WakeToRun semantics).
"""

from __future__ import annotations

import asyncio
import io
import logging
import smtplib
import ssl
import sys
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any

import httpx
from pydantic import AliasPath, BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas as rl_canvas
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

# ─────────────────────────────────────────────────────────────────────────────
# Logging Configuration
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("patr")


# ─────────────────────────────────────────────────────────────────────────────
# Configuration & Environment Settings
# ─────────────────────────────────────────────────────────────────────────────
class Settings(BaseSettings):
    """Runtime configuration model loaded from local environment variables or .env.

    Attributes:
        gmail_user: Gmail address used as SMTP sender and authentication principal.
        gmail_app_password: 16-character Google App Password for authenticated SMTP.
        report_to: Comma-separated list of recipient email addresses.
        nasa_api_key: NASA API authentication key (defaults to 'DEMO_KEY').
        earthquake_min_mag: Minimum Richter magnitude threshold for seismic alerts.
        earthquake_min_depth_km: Minimum hypocenter depth in km for deep-mantle alerts.
        buoy_min_wave_height_m: Significant wave height threshold in meters.
        buoy_min_pressure_drop_hpa: Rapid barometric pressure drop threshold in hPa.
        kp_storm_threshold: Geomagnetic disturbance K-index storm threshold (>= 5.0).
        launch_window_hours: Forward temporal lookahead window in hours for orbital launches.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    gmail_user: str
    gmail_app_password: str
    report_to: str

    nasa_api_key: str = "DEMO_KEY"

    earthquake_min_mag: float = 5.5
    earthquake_min_depth_km: float = 300.0
    buoy_min_wave_height_m: float = 10.0
    buoy_min_pressure_drop_hpa: float = 6.0
    kp_storm_threshold: float = 5.0
    launch_window_hours: int = 48

    @property
    def recipients(self) -> list[str]:
        """Parsed list of validated recipient email addresses.

        Returns:
            list[str]: Cleaned list of destination email addresses.
        """
        return [r.strip() for r in self.report_to.split(",") if r.strip()]


cfg = Settings()  # type: ignore[call-arg]


# ─────────────────────────────────────────────────────────────────────────────
# Resilient Retry Mechanics
# ─────────────────────────────────────────────────────────────────────────────
def _is_transient(exc: BaseException) -> bool:
    """Evaluate whether an exception represents a transient network or server error.

    Identifies standard rate-limiting (429), gateway/proxy failures (502, 503, 504),
    and socket timeouts or disconnections suitable for exponential backoff retries.

    Args:
        exc: The exception raised during execution.

    Returns:
        bool: True if the exception should trigger a retry attempt, False otherwise.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 502, 503, 504}
    return isinstance(exc, (httpx.TimeoutException, httpx.NetworkError))


_retry = retry(
    retry=retry_if_exception(_is_transient),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(4),
    reraise=True,
)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Telemetry Data Models (Pydantic V2)
# ─────────────────────────────────────────────────────────────────────────────

# ── 1. NASA Near-Earth Object Web Service (NeoWs) ────────────────────────────
class NeoVelocity(BaseModel):
    """Encapsulates relative approach velocity extracted from NeoWs payload."""

    kilometers_per_second: str = Field(
        validation_alias=AliasPath("kilometers_per_second")
    )

    @property
    def kps(self) -> float:
        """Relative velocity represented as a floating-point number in km/s."""
        return float(self.kilometers_per_second)


class NeoMissDistance(BaseModel):
    """Encapsulates nominal pass distance from Earth extracted from NeoWs payload."""

    kilometers: str = Field(validation_alias=AliasPath("kilometers"))

    @property
    def km(self) -> float:
        """Miss distance represented as a floating-point number in kilometers."""
        return float(self.kilometers)


class NeoObject(BaseModel):
    """Validated model representing a single Near-Earth Object approach event."""

    name: str
    is_hazardous: bool = Field(
        validation_alias=AliasPath("is_potentially_hazardous_asteroid")
    )
    velocity: NeoVelocity = Field(
        validation_alias=AliasPath("close_approach_data", 0, "relative_velocity")
    )
    miss_distance: NeoMissDistance = Field(
        validation_alias=AliasPath("close_approach_data", 0, "miss_distance")
    )


# ── 2. USGS Global Seismic Telemetry ─────────────────────────────────────────
class QuakeProperties(BaseModel):
    """Metadata and descriptive metrics for a USGS earthquake event."""

    mag: float | None = None
    place: str | None = None
    time: int | None = None

    @property
    def mag_safe(self) -> float:
        """Safe floating-point magnitude, defaulting to 0.0 when missing."""
        return self.mag or 0.0

    @property
    def utc_time(self) -> str:
        """Human-readable UTC timestamp converted from millisecond epoch."""
        if self.time is None:
            return "N/A"
        return datetime.fromtimestamp(self.time / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        )


class QuakeGeometry(BaseModel):
    """GeoJSON coordinate tuple containing longitude, latitude, and depth."""

    coordinates: list[float]

    @property
    def depth_km(self) -> float:
        """Hypocenter depth in kilometers located at index 2 of coordinates."""
        return self.coordinates[2] if len(self.coordinates) >= 3 else 0.0


class QuakeFeature(BaseModel):
    """Individual GeoJSON Feature entity for a detected earthquake."""

    properties: QuakeProperties
    geometry: QuakeGeometry


class QuakeFeed(BaseModel):
    """Root GeoJSON FeatureCollection wrapper for USGS seismic events."""

    features: list[QuakeFeature]


# ── 3. NOAA SWPC Planetary K-index ───────────────────────────────────────────
class KpReading(BaseModel):
    """Geomagnetic disturbance reading from NOAA SWPC 1-minute planetary feed."""

    time_tag: str
    kp: float

    @classmethod
    def from_row(cls, row: list[Any]) -> "KpReading":
        """Deserialize a tabular array row into a typed KpReading instance.

        Args:
            row: Array conforming to [time_tag, kp, estimated, noaa_scale].

        Returns:
            KpReading: Validated reading instance.
        """
        return cls(time_tag=str(row[0]), kp=float(row[1]))


# ── 4. NOAA NDBC Oceanographic Telemetry ──────────────────────────────────────
class BuoyReading(BaseModel):
    """Oceanographic observation record parsed from NDBC tabular fixed-width feeds."""

    station_id: str
    wvht_m: float | None = None
    pres_hpa: float | None = None
    pres_prev_hpa: float | None = None
    lat: float | None = None
    lon: float | None = None

    @property
    def pressure_drop(self) -> float | None:
        """Calculate barometric pressure delta against preceding observation.

        Returns:
            float | None: Positive delta indicating pressure drop in hPa, or None.
        """
        if self.pres_hpa is not None and self.pres_prev_hpa is not None:
            return self.pres_prev_hpa - self.pres_hpa
        return None


# ── 5. Launch Library 2 Orbital Manifest ─────────────────────────────────────
class LaunchAgency(BaseModel):
    """Aerospace manufacturing or operating agency entity."""

    name: str = "Unknown"


class LaunchRocket(BaseModel):
    """Launch vehicle system and variant configuration."""

    configuration: LaunchAgency = Field(default_factory=LaunchAgency)


class LaunchResult(BaseModel):
    """Individual orbital launch event details."""

    name: str
    net: str
    status_name: str = Field(
        validation_alias=AliasPath("status", "name"), default="Unknown"
    )
    rocket: LaunchRocket = Field(default_factory=LaunchRocket)
    pad_name: str = Field(
        validation_alias=AliasPath("pad", "name"), default="Unknown"
    )


class LaunchFeed(BaseModel):
    """Top-level container for upcoming launch missions from Launch Library 2."""

    results: list[LaunchResult]


# ── 6. NASA EPIC Earth Imagery ───────────────────────────────────────────────
class EPICEntry(BaseModel):
    """Metadata record for a natural-color Earth observation by DSCOVR EPIC."""

    identifier: str
    date: str
    image: str

    @property
    def archive_date(self) -> str:
        """Extract and format the date component into archive directory format (YYYY/MM/DD)."""
        d = datetime.strptime(self.date[:10], "%Y-%m-%d")
        return d.strftime("%Y/%m/%d")

    def png_url(self) -> str:
        """Synthesize the full HTTPS URL pointing to the lossless archive PNG.

        Returns:
            str: Resolved URL to the remote imagery asset.
        """
        return (
            f"https://epic.gsfc.nasa.gov/archive/natural/"
            f"{self.archive_date}/png/{self.image}.png"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Asynchronous Ingestion Coroutines
# ─────────────────────────────────────────────────────────────────────────────
TODAY = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

NASA_NEOWS_URL = (
    "https://api.nasa.gov/neo/rest/v1/feed"
    f"?start_date={TODAY}&end_date={TODAY}&api_key={cfg.nasa_api_key}"
)
USGS_QUAKE_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
NOAA_KP_URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
NDBC_STATION_LIST_URL = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"
LL2_URL = "https://ll.thespacedevs.com/2.3.0/launch/upcoming/?limit=10&format=json"
EPIC_METADATA_URL = "https://epic.gsfc.nasa.gov/api/natural"


@_retry
async def fetch_neo(client: httpx.AsyncClient) -> list[NeoObject]:
    """Fetch and filter potentially hazardous asteroids for the current UTC date.

    Args:
        client: Shared asynchronous HTTP client session.

    Returns:
        list[NeoObject]: Hazardous asteroids approaching Earth today.
    """
    log.info("Fetching NASA NeoWs …")
    resp = await client.get(NASA_NEOWS_URL, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    hazardous: list[NeoObject] = []
    for _date, objects in data.get("near_earth_objects", {}).items():
        for raw in objects:
            try:
                obj = NeoObject.model_validate(raw)
                if obj.is_hazardous:
                    hazardous.append(obj)
            except Exception as exc:
                log.debug("NeoWs parse skip: %s", exc)
    log.info("NeoWs: %d potentially hazardous NEOs today", len(hazardous))
    return hazardous


@_retry
async def fetch_quakes(client: httpx.AsyncClient) -> list[QuakeFeature]:
    """Retrieve and filter significant or deep global earthquakes from USGS.

    Args:
        client: Shared asynchronous HTTP client session.

    Returns:
        list[QuakeFeature]: Earthquakes matching magnitude or depth thresholds.
    """
    log.info("Fetching USGS earthquakes …")
    resp = await client.get(USGS_QUAKE_URL, timeout=20)
    resp.raise_for_status()
    feed = QuakeFeed.model_validate(resp.json())

    filtered = [
        f
        for f in feed.features
        if f.properties.mag_safe >= cfg.earthquake_min_mag
        or f.geometry.depth_km >= cfg.earthquake_min_depth_km
    ]
    log.info("Earthquakes: %d events match filter (of %d)", len(filtered), len(feed.features))
    return filtered


@_retry
async def fetch_kp(client: httpx.AsyncClient) -> list[KpReading]:
    """Retrieve planetary K-index readings from NOAA SWPC and filter storm levels.

    Args:
        client: Shared asynchronous HTTP client session.

    Returns:
        list[KpReading]: Readings meeting or exceeding storm threshold (Kp >= 5.0).
    """
    log.info("Fetching NOAA SWPC Kp index …")
    resp = await client.get(NOAA_KP_URL, timeout=20)
    resp.raise_for_status()
    rows = resp.json()

    readings: list[KpReading] = []
    for row in rows[1:]:  # skip header row
        try:
            readings.append(KpReading.from_row(row))
        except Exception as exc:
            log.debug("Kp parse skip: %s", exc)

    storm = [r for r in readings if r.kp >= cfg.kp_storm_threshold]
    log.info("SWPC Kp: %d storm-level readings (Kp >= %.1f) in feed", len(storm), cfg.kp_storm_threshold)
    return storm


_DART_PREFIXES = {"32", "43", "46", "51", "52", "55"}


def _is_dart(station_id: str) -> bool:
    """Determine whether a station ID represents a DART tsunami monitoring buoy.

    DART (Deep-ocean Assessment and Reporting of Tsunamis) buoys record bottom
    pressure rather than surface wave height spectra, requiring exclusion from
    surface sea-state anomaly filters.

    Args:
        station_id: Alphanumeric or numeric NDBC station identifier.

    Returns:
        bool: True if the identifier matches known DART network naming conventions.
    """
    return len(station_id) == 5 and station_id[:2] in _DART_PREFIXES


def _clean(value: str) -> float | None:
    """Sanitize and parse NDBC meteorological values, filtering sentinel flags.

    Args:
        value: Raw text token from NDBC fixed-width tabular streams.

    Returns:
        float | None: Parsed float if valid, or None if sentinel ('MM', '99.0').
    """
    _SENTINELS = {"MM", "99.00", "999.0", "9999.0", "99.0", "9999", "999", "99"}
    v = value.strip()
    if v in _SENTINELS:
        return None
    try:
        f = float(v)
        if f >= 99.0 and v in {"99.0", "99.00", "999.0", "9999.0"}:
            return None
        return f
    except ValueError:
        return None


@_retry
async def fetch_buoys(client: httpx.AsyncClient) -> list[BuoyReading]:
    """Ingest and parse the NOAA NDBC global latest observations text matrix.

    Parses fixed-width space-delimited text directly in memory, strips sentinel
    values, filters out bottom-pressure DART buoys, and identifies extreme sea states
    or rapid barometric drops.

    Args:
        client: Shared asynchronous HTTP client session.

    Returns:
        list[BuoyReading]: Station readings exhibiting oceanic or pressure anomalies.
    """
    log.info("Fetching NDBC buoy data …")
    resp = await client.get(NDBC_STATION_LIST_URL, timeout=30)
    resp.raise_for_status()

    lines = resp.text.splitlines()
    if len(lines) < 3:
        log.warning("NDBC response too short")
        return []

    headers = lines[0].lstrip("#").split()

    def col(name: str) -> int | None:
        try:
            return headers.index(name)
        except ValueError:
            return None

    i_station = col("STN") or 0
    i_wvht = col("WVHT")
    i_pres = col("PRES")

    anomalies: list[BuoyReading] = []
    prev_pres: dict[str, float] = {}

    for line in lines[2:]:
        if not line.strip() or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) <= max(filter(None, [i_station, i_wvht, i_pres]), default=0):
            continue

        try:
            station_id = parts[i_station]
            wvht = _clean(parts[i_wvht]) if i_wvht is not None and i_wvht < len(parts) else None
            pres = _clean(parts[i_pres]) if i_pres is not None and i_pres < len(parts) else None
        except (IndexError, TypeError):
            continue

        reading = BuoyReading(
            station_id=station_id,
            wvht_m=wvht,
            pres_hpa=pres,
            pres_prev_hpa=prev_pres.get(station_id),
        )

        if pres is not None:
            prev_pres[station_id] = pres

        wave_trigger = (
            not _is_dart(station_id)
            and wvht is not None
            and wvht >= cfg.buoy_min_wave_height_m
        )
        pdrop = reading.pressure_drop
        pressure_trigger = pdrop is not None and pdrop >= cfg.buoy_min_pressure_drop_hpa

        if wave_trigger or pressure_trigger:
            anomalies.append(reading)

    log.info("NDBC: %d buoy anomalies detected", len(anomalies))
    return anomalies


@_retry
async def fetch_launches(client: httpx.AsyncClient) -> list[LaunchResult]:
    """Retrieve upcoming orbital spaceflight missions scheduled within lookahead window.

    Args:
        client: Shared asynchronous HTTP client session.

    Returns:
        list[LaunchResult]: Launch missions scheduled inside configured time window.
    """
    log.info("Fetching Launch Library 2 upcoming launches …")
    resp = await client.get(LL2_URL, timeout=20)
    resp.raise_for_status()
    feed = LaunchFeed.model_validate(resp.json())

    now = datetime.now(tz=timezone.utc)
    window: list[LaunchResult] = []
    for launch in feed.results:
        try:
            net_dt = datetime.fromisoformat(launch.net.replace("Z", "+00:00"))
            delta_h = (net_dt - now).total_seconds() / 3600
            if 0 <= delta_h <= cfg.launch_window_hours:
                window.append(launch)
        except ValueError:
            continue

    log.info("Launches: %d within next %dh", len(window), cfg.launch_window_hours)
    return window


@_retry
async def fetch_epic_image(client: httpx.AsyncClient) -> tuple[EPICEntry, io.BytesIO]:
    """Fetch latest DSCOVR EPIC metadata and download full-resolution natural Earth imagery.

    The acquired PNG payload is read directly into a volatile in-memory `io.BytesIO` buffer.
    Zero bytes touch secondary storage.

    Args:
        client: Shared asynchronous HTTP client session.

    Returns:
        tuple[EPICEntry, io.BytesIO]: Metadata descriptor and in-memory image buffer.

    Raises:
        ValueError: If EPIC metadata API returns an empty catalog.
    """
    log.info("Fetching NASA EPIC metadata …")
    meta_resp = await client.get(EPIC_METADATA_URL, timeout=20)
    meta_resp.raise_for_status()

    entries = [EPICEntry.model_validate(e) for e in meta_resp.json()]
    if not entries:
        raise ValueError("EPIC metadata returned empty list")

    latest = entries[0]
    png_url = latest.png_url()
    log.info("Downloading EPIC image: %s", png_url)

    img_resp = await client.get(png_url, timeout=60, follow_redirects=True)
    img_resp.raise_for_status()

    buf = io.BytesIO(img_resp.content)
    buf.seek(0)
    log.info("EPIC image downloaded (%d bytes) into memory", len(img_resp.content))
    return latest, buf


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Orchestrator Coroutine
# ─────────────────────────────────────────────────────────────────────────────
async def ingest_all() -> dict[str, Any]:
    """Execute concurrent telemetry acquisition across all external providers.

    Employs `asyncio.gather` with `return_exceptions=True` to guarantee partial
    fault tolerance: failure in any single upstream provider does not invalidate
    remaining intelligence channels.

    Returns:
        dict[str, Any]: Dictionary containing deserialized records and image buffers.
    """
    async with httpx.AsyncClient(
        headers={"User-Agent": "PlanetaryAnomalyReport/1.0 (+https://github.com/blairmichaelg/planetary-anomaly-report)"},
        follow_redirects=True,
    ) as client:
        (
            neo_result,
            quake_result,
            kp_result,
            buoy_result,
            launch_result,
            epic_result,
        ) = await asyncio.gather(
            fetch_neo(client),
            fetch_quakes(client),
            fetch_kp(client),
            fetch_buoys(client),
            fetch_launches(client),
            fetch_epic_image(client),
            return_exceptions=True,
        )

    data: dict[str, Any] = {}

    def _unwrap(key: str, result: Any, default: Any) -> None:
        if isinstance(result, BaseException):
            log.error("Source '%s' failed: %s", key, result)
            data[key] = default
        else:
            data[key] = result

    _unwrap("neo", neo_result, [])
    _unwrap("quakes", quake_result, [])
    _unwrap("kp", kp_result, [])
    _unwrap("buoys", buoy_result, [])
    _unwrap("launches", launch_result, [])

    if isinstance(epic_result, BaseException):
        log.error("EPIC fetch failed: %s", epic_result)
        data["epic_entry"] = None
        data["epic_buf"] = None
    else:
        data["epic_entry"], data["epic_buf"] = epic_result

    return data


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — In-Memory Vector PDF Synthesis (ReportLab Canvas)
# ─────────────────────────────────────────────────────────────────────────────

C_BG = colors.HexColor("#0B0D17")
C_PANEL = colors.HexColor("#141928")
C_ACCENT = colors.HexColor("#4FC3F7")
C_WARN = colors.HexColor("#FF7043")
C_TEXT = colors.HexColor("#E8EAF6")
C_DIM = colors.HexColor("#78909C")
C_GREEN = colors.HexColor("#69F0AE")
C_YELLOW = colors.HexColor("#FFEE58")

PAGE_W, PAGE_H = A4
MARGIN = 14 * mm
INNER_W = PAGE_W - 2 * MARGIN


def _filled_rect(
    c: rl_canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    fill: colors.Color,
    stroke: colors.Color | None = None,
) -> None:
    """Draw a filled and optionally stroked rectangle on the PDF canvas.

    Args:
        c: Target ReportLab canvas instance.
        x: Lower-left X coordinate in points.
        y: Lower-left Y coordinate in points.
        w: Width of rectangle in points.
        h: Height of rectangle in points.
        fill: Fill color.
        stroke: Optional border stroke color.
    """
    c.setFillColor(fill)
    if stroke:
        c.setStrokeColor(stroke)
        c.setLineWidth(0.5)
        c.rect(x, y, w, h, fill=1, stroke=1)
    else:
        c.rect(x, y, w, h, fill=1, stroke=0)


def _label(
    c: rl_canvas.Canvas,
    text: str,
    x: float,
    y: float,
    size: float = 7,
    colour: colors.Color = C_DIM,
    bold: bool = False,
) -> None:
    """Render a single text label with typographic formatting.

    Args:
        c: Target ReportLab canvas instance.
        text: String content to print.
        x: Baseline X coordinate in points.
        y: Baseline Y coordinate in points.
        size: Font size in points.
        colour: Text fill color.
        bold: Whether to render using Helvetica-Bold or regular Helvetica.
    """
    c.setFillColor(colour)
    c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
    c.drawString(x, y, text)


def _truncate(text: str, max_chars: int) -> str:
    """Ellipsize string values exceeding a maximum character length.

    Args:
        text: Raw input string.
        max_chars: Maximum allowable character length including ellipsis.

    Returns:
        str: Truncated string bounded to max_chars.
    """
    return text if len(text) <= max_chars else text[: max_chars - 1] + "…"


def _section_header(
    c: rl_canvas.Canvas,
    title: str,
    icon: str,
    x: float,
    y: float,
    w: float,
) -> float:
    """Render a styled section header banner bar across a designated column.

    Args:
        c: Target ReportLab canvas instance.
        title: Section title text.
        icon: Unicode icon glyph.
        x: Left X origin in points.
        y: Top Y origin in points.
        w: Width of banner in points.

    Returns:
        float: Updated baseline Y coordinate positioned beneath the header bar.
    """
    bar_h = 11
    _filled_rect(c, x, y - bar_h, w, bar_h, C_PANEL)
    _label(c, f"{icon}  {title}", x + 4, y - 8.5, size=7.5, colour=C_ACCENT, bold=True)
    return y - bar_h


def _row_bg(
    c: rl_canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    idx: int,
) -> None:
    """Render alternating subtle background banding for tabular rows.

    Args:
        c: Target ReportLab canvas instance.
        x: Row origin X coordinate.
        y: Row origin Y coordinate.
        w: Row width in points.
        h: Row height in points.
        idx: Zero-based row index used to calculate modulo alternation.
    """
    col = colors.HexColor("#0F1320") if idx % 2 == 0 else C_BG
    _filled_rect(c, x, y, w, h, col)


def build_pdf(data: dict[str, Any]) -> bytes:
    """Synthesize raw telemetry and imagery into a single-page PDF dashboard.

    All PDF graphics, typography, tables, and rasters are composed directly inside
    an in-memory `io.BytesIO` stream using absolute canvas coordinates to enforce
    strict layout containment and eliminate Platypus reflow crashes.

    Args:
        data: Ingestion dictionary containing models and in-memory image buffers.

    Returns:
        bytes: Complete, valid PDF binary stream.
    """
    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=A4)

    # Background
    _filled_rect(c, 0, 0, PAGE_W, PAGE_H, C_BG)

    # Header banner
    banner_h = 28 * mm
    _filled_rect(c, 0, PAGE_H - banner_h, PAGE_W, banner_h, C_PANEL)

    # Title & Timestamp
    c.setFillColor(C_ACCENT)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(MARGIN, PAGE_H - 18 * mm, "🌍  PLANETARY ANOMALY & THREAT REPORT")

    c.setFillColor(C_DIM)
    c.setFont("Helvetica", 8)
    run_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    c.drawRightString(PAGE_W - MARGIN, PAGE_H - 18 * mm, f"Generated: {run_ts}")

    c.setFillColor(C_TEXT)
    c.setFont("Helvetica", 8.5)
    c.drawString(MARGIN, PAGE_H - 24 * mm, "Real-time synthesis · NASA · USGS · NOAA · Launch Library 2")

    # Spatial Column Partitioning
    top_y = PAGE_H - banner_h - 4 * mm
    img_w = 78 * mm
    img_h = 78 * mm
    col1_w = INNER_W - img_w - 4 * mm

    # NASA EPIC Earth Raster Placement
    epic_buf: io.BytesIO | None = data.get("epic_buf")
    epic_entry: EPICEntry | None = data.get("epic_entry")

    img_x = MARGIN + col1_w + 4 * mm
    img_y = top_y - img_h

    if epic_buf:
        try:
            from PIL import Image as PILImage  # noqa: PLC0415

            pil_img = PILImage.open(epic_buf)
            pil_img.thumbnail((int(img_w * 3.78), int(img_h * 3.78)))
            thumb_buf = io.BytesIO()
            pil_img.save(thumb_buf, format="PNG")
            thumb_buf.seek(0)

            from reportlab.lib.utils import ImageReader  # noqa: PLC0415

            ir = ImageReader(thumb_buf)
            c.drawImage(
                ir,
                img_x,
                img_y,
                width=img_w,
                height=img_h,
                preserveAspectRatio=True,
                mask="auto",
            )
        except Exception as exc:
            log.warning("Could not embed EPIC image: %s", exc)
            epic_buf = None

    _filled_rect(
        c,
        img_x,
        img_y,
        img_w,
        img_h,
        fill=C_BG,
        stroke=C_ACCENT if not epic_buf else None,
    )
    if not epic_buf:
        _label(c, "EPIC image unavailable", img_x + 6, img_y + img_h / 2, 7, C_DIM)

    if epic_entry:
        c.setFillColor(C_DIM)
        c.setFont("Helvetica", 6.5)
        cap = f"NASA EPIC · {epic_entry.date[:16]} UTC"
        c.drawCentredString(img_x + img_w / 2, img_y - 5, cap)

    # Left Column Sections
    cx = MARGIN
    cy = top_y
    cw = col1_w
    row_h = 8.5

    # 1. Near-Earth Objects
    neo_list: list[NeoObject] = data.get("neo", [])
    cy = _section_header(c, "NEAR-EARTH OBJECTS  (Potentially Hazardous)", "☄️", cx, cy, cw)
    cy -= 1

    if neo_list:
        hdr = ["Asteroid", "Velocity (km/s)", "Miss Dist (km)"]
        col_w = [cw * 0.50, cw * 0.25, cw * 0.25]
        _filled_rect(c, cx, cy - row_h, cw, row_h, colors.HexColor("#1C2340"))
        for j, h in enumerate(hdr):
            xj = cx + sum(col_w[:j]) + 2
            _label(c, h, xj, cy - 6.5, 6, C_ACCENT, bold=True)
        cy -= row_h

        for i, neo in enumerate(neo_list[:8]):
            _row_bg(c, cx, cy - row_h, cw, row_h, i)
            vals = [
                _truncate(neo.name, 38),
                f"{neo.velocity.kps:.3f}",
                f"{neo.miss_distance.km:,.0f}",
            ]
            for j, v in enumerate(vals):
                xj = cx + sum(col_w[:j]) + 2
                colour = C_WARN if j == 0 else C_TEXT
                _label(c, v, xj, cy - 6.5, 6, colour)
            cy -= row_h
    else:
        _label(c, "No potentially hazardous NEOs detected today.", cx + 2, cy - 7, 7, C_GREEN)
        cy -= 10

    cy -= 3

    # 2. Geomagnetic Storm Activity
    kp_list: list[KpReading] = data.get("kp", [])
    cy = _section_header(c, "GEOMAGNETIC STORM ACTIVITY  (Kp ≥ 5.0)", "🌩️", cx, cy, cw)
    cy -= 1

    if kp_list:
        max_kp = max(r.kp for r in kp_list)
        kp_colour = C_WARN if max_kp >= 7 else C_YELLOW
        _label(
            c,
            f"Peak Kp = {max_kp:.1f}   ({len(kp_list)} readings at storm level in last 24h)",
            cx + 2,
            cy - 7,
            7,
            kp_colour,
            bold=True,
        )
        cy -= 10
        for i, kpr in enumerate(sorted(kp_list, key=lambda r: r.kp, reverse=True)[:5]):
            _row_bg(c, cx, cy - row_h, cw, row_h, i)
            _label(c, f"{kpr.time_tag}   Kp = {kpr.kp:.1f}", cx + 4, cy - 6.5, 6.5, C_TEXT)
            cy -= row_h
    else:
        _label(c, "No geomagnetic storm activity in last 24h.", cx + 2, cy - 7, 7, C_GREEN)
        cy -= 10

    cy -= 3

    # 3. Seismic Events
    quake_list: list[QuakeFeature] = data.get("quakes", [])
    cy = _section_header(
        c,
        f"SEISMIC EVENTS  (Mag ≥ {cfg.earthquake_min_mag} or Depth ≥ {cfg.earthquake_min_depth_km:.0f} km)",
        "🔴",
        cx,
        cy,
        cw,
    )
    cy -= 1

    if quake_list:
        hdr = ["Location", "Mag", "Depth (km)", "Time (UTC)"]
        col_w = [cw * 0.38, cw * 0.10, cw * 0.16, cw * 0.36]
        _filled_rect(c, cx, cy - row_h, cw, row_h, colors.HexColor("#1C2340"))
        for j, h in enumerate(hdr):
            xj = cx + sum(col_w[:j]) + 2
            _label(c, h, xj, cy - 6.5, 6, C_ACCENT, bold=True)
        cy -= row_h

        for i, qk in enumerate(sorted(quake_list, key=lambda q: q.properties.mag_safe, reverse=True)[:10]):
            _row_bg(c, cx, cy - row_h, cw, row_h, i)
            depth = qk.geometry.depth_km
            mag = qk.properties.mag_safe
            mag_col = C_WARN if mag >= 7 else (C_YELLOW if mag >= 6 else C_TEXT)
            vals = [
                _truncate(qk.properties.place or "Unknown", 30),
                f"{mag:.1f}",
                f"{depth:.0f}",
                qk.properties.utc_time,
            ]
            for j, v in enumerate(vals):
                xj = cx + sum(col_w[:j]) + 2
                colour = mag_col if j == 1 else C_TEXT
                _label(c, v, xj, cy - 6.5, 6, colour)
            cy -= row_h
    else:
        _label(c, "No significant seismic events in current feed.", cx + 2, cy - 7, 7, C_GREEN)
        cy -= 10

    cy -= 3

    # 4. Oceanic Buoy Anomalies
    buoy_list: list[BuoyReading] = data.get("buoys", [])
    cy = _section_header(
        c,
        f"OCEANIC ANOMALIES  (Wave ≥ {cfg.buoy_min_wave_height_m:.0f}m or ΔP ≥ {cfg.buoy_min_pressure_drop_hpa:.0f} hPa)",
        "🌊",
        cx,
        cy,
        cw,
    )
    cy -= 1

    if buoy_list:
        hdr = ["Station", "Wave Ht (m)", "Pres (hPa)", "ΔP (hPa)"]
        col_w = [cw * 0.20, cw * 0.22, cw * 0.22, cw * 0.36]
        _filled_rect(c, cx, cy - row_h, cw, row_h, colors.HexColor("#1C2340"))
        for j, h in enumerate(hdr):
            xj = cx + sum(col_w[:j]) + 2
            _label(c, h, xj, cy - 6.5, 6, C_ACCENT, bold=True)
        cy -= row_h

        for i, buoy in enumerate(buoy_list[:8]):
            _row_bg(c, cx, cy - row_h, cw, row_h, i)
            pdrop = buoy.pressure_drop
            vals = [
                buoy.station_id,
                f"{buoy.wvht_m:.1f}" if buoy.wvht_m is not None else "—",
                f"{buoy.pres_hpa:.1f}" if buoy.pres_hpa is not None else "—",
                f"{pdrop:.1f}" if pdrop is not None else "—",
            ]
            col_colours = [
                C_TEXT,
                C_WARN if (buoy.wvht_m or 0) >= cfg.buoy_min_wave_height_m else C_TEXT,
                C_TEXT,
                C_WARN if (pdrop or 0) >= cfg.buoy_min_pressure_drop_hpa else C_TEXT,
            ]
            for j, v in enumerate(vals):
                xj = cx + sum(col_w[:j]) + 2
                _label(c, v, xj, cy - 6.5, 6, col_colours[j])
            cy -= row_h
    else:
        _label(c, "No oceanic anomalies in NDBC feed.", cx + 2, cy - 7, 7, C_GREEN)
        cy -= 10

    cy -= 3

    # 5. Orbital Launch Manifest
    launch_list: list[LaunchResult] = data.get("launches", [])
    cy = _section_header(c, f"UPCOMING LAUNCHES  (Next {cfg.launch_window_hours}h)", "🚀", cx, cy, cw)
    cy -= 1

    if launch_list:
        hdr = ["Mission", "Vehicle", "Pad", "NET (UTC)"]
        col_w = [cw * 0.28, cw * 0.20, cw * 0.22, cw * 0.30]
        _filled_rect(c, cx, cy - row_h, cw, row_h, colors.HexColor("#1C2340"))
        for j, h in enumerate(hdr):
            xj = cx + sum(col_w[:j]) + 2
            _label(c, h, xj, cy - 6.5, 6, C_ACCENT, bold=True)
        cy -= row_h

        for i, launch in enumerate(launch_list[:6]):
            _row_bg(c, cx, cy - row_h, cw, row_h, i)
            net_str = launch.net[:16].replace("T", " ") if len(launch.net) >= 16 else launch.net
            vals = [
                _truncate(launch.name, 22),
                _truncate(launch.rocket.configuration.name, 15),
                _truncate(launch.pad_name, 18),
                net_str,
            ]
            for j, v in enumerate(vals):
                xj = cx + sum(col_w[:j]) + 2
                _label(c, v, xj, cy - 6.5, 6, C_GREEN if j == 0 else C_TEXT)
            cy -= row_h
    else:
        _label(c, f"No launches scheduled in the next {cfg.launch_window_hours}h.", cx + 2, cy - 7, 7, C_DIM)
        cy -= 10

    # Footer Attribution
    footer_y = 8 * mm
    c.setStrokeColor(C_PANEL)
    c.setLineWidth(0.5)
    c.line(MARGIN, footer_y + 5, PAGE_W - MARGIN, footer_y + 5)
    _label(
        c,
        "Data sources: NASA NeoWs · USGS ANSS · NOAA SWPC · NDBC · Launch Library 2 · NASA EPIC",
        MARGIN,
        footer_y,
        6,
        C_DIM,
    )
    c.setFillColor(C_DIM)
    c.setFont("Helvetica", 6)
    c.drawRightString(PAGE_W - MARGIN, footer_y, "CONFIDENTIAL — INTERNAL USE ONLY")

    c.showPage()
    c.save()

    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — Authenticated Encrypted SMTP Dispatch
# ─────────────────────────────────────────────────────────────────────────────
def send_report(pdf_bytes: bytes) -> None:
    """Package the in-memory PDF buffer into a MIME message and dispatch via TLS.

    Uses `email.message.EmailMessage` to attach raw PDF bytes without landing on disk.
    Connects to Gmail SMTP port 587, upgrades connection via STARTTLS using host
    OS root certificates, authenticates with Google App Password, and dispatches.

    Args:
        pdf_bytes: Raw binary stream of the assembled PDF dashboard.
    """
    subject_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    subject = f"🌍 Planetary Anomaly & Threat Report — {subject_date}"

    msg = EmailMessage()
    msg["From"] = cfg.gmail_user
    msg["To"] = ", ".join(cfg.recipients)
    msg["Subject"] = subject

    msg.set_content(
        f"Planetary Anomaly & Threat Report\n"
        f"Generated: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        f"Please see the attached PDF for the full planetary intelligence briefing.\n\n"
        f"Data sources: NASA NeoWs · USGS Earthquakes · NOAA SWPC Kp · "
        f"NDBC Buoys · Launch Library 2 · NASA EPIC\n"
    )

    filename = f"planetary_report_{subject_date}.pdf"
    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=filename,
    )

    tls_ctx = ssl.create_default_context()

    log.info("Connecting to smtp.gmail.com:587 …")
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as smtp:
        smtp.ehlo()
        smtp.starttls(context=tls_ctx)
        smtp.ehlo()
        smtp.login(cfg.gmail_user, cfg.gmail_app_password)
        smtp.send_message(msg)

    log.info("Report dispatched to: %s", ", ".join(cfg.recipients))


# ─────────────────────────────────────────────────────────────────────────────
# Process Lifecycle Entrypoint
# ─────────────────────────────────────────────────────────────────────────────
async def _main() -> None:
    """Async orchestration entrypoint executing ingestion, synthesis, and dispatch."""
    log.info("=== Planetary Anomaly & Threat Report starting ===")
    log.info("Report date: %s", TODAY)

    # Phase 1: Ingestion
    data = await ingest_all()

    log.info(
        "Ingestion complete: %d NEOs | %d quakes | %d Kp storms | "
        "%d buoy anomalies | %d launches | EPIC=%s",
        len(data.get("neo", [])),
        len(data.get("quakes", [])),
        len(data.get("kp", [])),
        len(data.get("buoys", [])),
        len(data.get("launches", [])),
        "OK" if data.get("epic_buf") else "FAILED",
    )

    # Phase 2: In-Memory PDF Synthesis
    log.info("Building PDF dashboard …")
    pdf_bytes = build_pdf(data)
    log.info("PDF assembled: %d bytes (volatile memory, zero disk writes)", len(pdf_bytes))

    # Phase 3: Secure Dispatch
    log.info("Dispatching report …")
    send_report(pdf_bytes)

    log.info("=== Done ===")


def main() -> None:
    """Synchronous runtime entrypoint invoking the event loop."""
    asyncio.run(_main())


if __name__ == "__main__":
    main()
