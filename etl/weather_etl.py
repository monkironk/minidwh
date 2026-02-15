# etl/weather_etl.py

import requests
from datetime import datetime, timedelta
from statistics import mean

from db import get_conn


def month_to_dateid(month: str) -> int:
    # 2025-11 -> 202511
    y, m = month.split("-")
    return int(y) * 100 + int(m)


def month_first_date(month: str) -> str:
   # 2025-11 -> 2025-11-01
    return f"{month}-01"


def month_end_date(month: str) -> str:
    # Letzter Tag des Monats zum Beispiel 2025-11 -> 2025-11-30
    dt = datetime.strptime(month + "-01", "%Y-%m-%d")
    if dt.month == 12:
        next_month = dt.replace(year=dt.year + 1, month=1, day=1)
    else:
        next_month = dt.replace(month=dt.month + 1, day=1)
    last_day = (next_month - timedelta(days=1)).date()
    return last_day.isoformat()


def load_weather_month(lat: float, lng: float, month: str) -> dict:
    """
    aggregiert monatsdaten in 6h Intervall

    Erwartete Spalten in weather_dim:
      - weatherID, dateID
      - avgTemp, minTemp, maxTemp
      - totalRainMM, totalSnowCM, daylightRatio
    """
    start_date = month_first_date(month)
    end_date = month_end_date(month)

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lng}"
        f"&start_date={start_date}&end_date={end_date}"
        "&hourly=temperature_2m,is_day,rain,snowfall"
        "&temporal_resolution=hourly_6"
    )

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    hourly = data.get("hourly") or {}
    temps = hourly.get("temperature_2m") or []
    rain = hourly.get("rain") or []
    snow = hourly.get("snowfall") or []
    is_day = hourly.get("is_day") or []

    if not temps:
        raise ValueError(f"Keine temperature_2m Daten erhalten für {month}. URL: {url}")

    # monatliche Aggregation
    avg_temp = float(mean(temps))
    min_temp = float(min(temps))
    max_temp = float(max(temps))

    total_rain = float(sum(rain)) if rain else 0.0
    total_snow = float(sum(snow)) if snow else 0.0
    daylight_ratio = float(mean(is_day)) if is_day else 0.0

    date_id = month_to_dateid(month)  # YYYYMM
    year = int(month.split("-")[0])
    month_num = int(month.split("-")[1])

    with get_conn() as conn:
        with conn.cursor() as cur:
            # date_dim upsert
            cur.execute(
                """
                INSERT INTO date_dim (dateID, date, year, month, day)
                VALUES (%s, %s::date, %s, %s, 1)
                ON CONFLICT (dateID) DO NOTHING;
                """,
                (date_id, start_date, year, month_num),
            )

            # weather_dim upsert (ohne rainy/snowy)
            cur.execute(
                """
                INSERT INTO weather_dim
                  (weatherID, dateID, avgTemp, minTemp, maxTemp,
                   totalRainMM, totalSnowCM, daylightRatio)
                VALUES
                  (%s, %s, %s, %s, %s,
                   %s, %s, %s)
                ON CONFLICT (weatherID) DO UPDATE
                SET dateID        = EXCLUDED.dateID,
                    avgTemp       = EXCLUDED.avgTemp,
                    minTemp       = EXCLUDED.minTemp,
                    maxTemp       = EXCLUDED.maxTemp,
                    totalRainMM   = EXCLUDED.totalRainMM,
                    totalSnowCM   = EXCLUDED.totalSnowCM,
                    daylightRatio = EXCLUDED.daylightRatio;
                """,
                (
                    date_id,
                    date_id,
                    avg_temp,
                    min_temp,
                    max_temp,
                    total_rain,
                    total_snow,
                    daylight_ratio,
                ),
            )

    return {
        "month": month,
        "dateID": date_id,
        "avgTemp": avg_temp,
        "minTemp": min_temp,
        "maxTemp": max_temp,
        "totalRainMM": total_rain,
        "totalSnowCM": total_snow,
        "daylightRatio": daylight_ratio,
        "api_timezone": data.get("timezone"),
    }
