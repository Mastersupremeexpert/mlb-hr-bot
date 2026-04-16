"""
MLB Home Run Bot — Weather Ingestion
Uses Open-Meteo (free, no API key required).
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import math
import requests
from datetime import date, datetime, timezone

from config import WEATHER_BASE
from data.schema import get_connection, execute, fetchall

log = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def wind_direction_label(deg: float, lf_orientation: float = 220.0) -> str:
    """
    Classify wind as out-to-CF, in-from-CF, cross-wind, etc.
    lf_orientation: rough compass bearing to CF (park-specific; default ~220° SW).
    """
    cf_bearing = lf_orientation % 360
    diff = (deg - cf_bearing + 360) % 360
    if diff < 45 or diff > 315:
        return "out_to_cf"
    elif 135 < diff < 225:
        return "in_from_cf"
    elif 45 <= diff <= 135:
        return "cross_wind_left"
    else:
        return "cross_wind_right"


def air_density_index(temp_f: float, humidity_pct: float, altitude_ft: float) -> float:
    """
    Relative air density index (1.0 = sea level standard).
    Lower = less resistance = more home run carry.
    """
    temp_c = (temp_f - 32) * 5 / 9
    altitude_m = altitude_ft * 0.3048
    # Barometric pressure approximation (Pa)
    P = 101325 * math.exp(-0.0000225577 * altitude_m)
    # Saturation vapor pressure (Magnus formula)
    es = 611.2 * math.exp(17.67 * temp_c / (temp_c + 243.5))
    Pv = (humidity_pct / 100) * es
    Pd = P - Pv
    # Density relative to standard sea level (1.225 kg/m³)
    rho = (Pd * 0.028964 + Pv * 0.018016) / (8.314 * (temp_c + 273.15))
    return round(rho / 1.225, 4)


def fetch_weather_for_game(game_pk: int, venue_id: int, lat: float, lon: float,
                            altitude_ft: float, game_time_utc: str,
                            roof_type: str = "open") -> dict | None:
    """Fetch forecast and compute environment features."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m,winddirection_10m",
        "temperature_unit": "fahrenheit",
        "windspeed_unit": "mph",
        "timezone": "UTC",
        "forecast_days": 3,
    }
    try:
        resp = requests.get(WEATHER_BASE, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Weather fetch failed for venue {venue_id}: {e}")
        return None

    # Match the game hour
    try:
        game_dt = datetime.fromisoformat(game_time_utc.replace("Z", "+00:00"))
        game_hour_str = game_dt.strftime("%Y-%m-%dT%H:00")
    except Exception:
        game_hour_str = None

    hours = data.get("hourly", {})
    times = hours.get("time", [])
    temps = hours.get("temperature_2m", [])
    humids = hours.get("relativehumidity_2m", [])
    winds = hours.get("windspeed_10m", [])
    wind_dirs = hours.get("winddirection_10m", [])

    idx = 0
    if game_hour_str:
        try:
            idx = times.index(game_hour_str)
        except ValueError:
            # Use closest available
            for i, t in enumerate(times):
                if t >= game_hour_str:
                    idx = i
                    break

    temp_f = temps[idx] if temps else 70.0
    humid = humids[idx] if humids else 50.0
    wind_spd = winds[idx] if winds else 0.0
    wind_dir = wind_dirs[idx] if wind_dirs else 0.0

    roof_open = 1 if roof_type == "open" else 0
    # Retractable roofs default closed if cold or rainy
    if roof_type == "retractable" and temp_f < 55:
        roof_open = 0
    elif roof_type == "retractable":
        roof_open = 1

    adi = air_density_index(temp_f, humid, altitude_ft if roof_open else 0)
    wind_label = wind_direction_label(wind_dir)

    return {
        "game_pk": game_pk,
        "venue_id": venue_id,
        "fetched_at": _now_utc(),
        "game_time_local": game_time_utc,
        "temperature_f": round(temp_f, 1),
        "humidity_pct": round(humid, 1),
        "wind_speed_mph": round(wind_spd, 1),
        "wind_dir_deg": round(wind_dir, 1),
        "wind_dir_label": wind_label,
        "roof_open": roof_open,
        "air_density_idx": adi,
    }


def run_ingest_weather(game_date: date | None = None):
    """Fetch weather for all games on a date."""
    if game_date is None:
        game_date = date.today()

    conn = get_connection()
    games = fetchall(conn, """
        SELECT g.game_pk, g.venue_id, g.game_time_utc,
               v.latitude, v.longitude, v.altitude_ft, v.roof_type
        FROM games g
        LEFT JOIN venues v ON g.venue_id = v.venue_id
        WHERE g.game_date=?
    """, (game_date.strftime("%Y-%m-%d"),))

    count = 0
    for g in games:
        if not g["latitude"]:
            continue
        w = fetch_weather_for_game(
            game_pk=g["game_pk"],
            venue_id=g["venue_id"],
            lat=g["latitude"],
            lon=g["longitude"],
            altitude_ft=g["altitude_ft"] or 0,
            game_time_utc=g["game_time_utc"] or "",
            roof_type=g["roof_type"] or "open",
        )
        if w:
            execute(conn, """
                INSERT INTO environment_features(
                    game_pk,venue_id,fetched_at,game_time_local,temperature_f,
                    humidity_pct,wind_speed_mph,wind_dir_deg,wind_dir_label,
                    roof_open,air_density_idx
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT DO NOTHING
            """, (
                w["game_pk"], w["venue_id"], w["fetched_at"], w["game_time_local"],
                w["temperature_f"], w["humidity_pct"], w["wind_speed_mph"],
                w["wind_dir_deg"], w["wind_dir_label"], w["roof_open"], w["air_density_idx"],
            ))
            count += 1

    conn.commit()
    conn.close()
    log.info(f"Weather ingested for {count} games.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_ingest_weather()
