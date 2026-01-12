import requests
import psycopg2
from datetime import datetime
from statistics import mean

URL = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude=51.758204&longitude=-1.255826"
    "&start_date=2025-12-01&end_date=2025-12-31"
    "&hourly=temperature_2m,is_day,rain,snowfall"
    "&temporal_resolution=hourly_6"
)

DB = {
    "host": "localhost",
    "port": 5432,
    "dbname": "minidwh",
    "user": "",
    "password": "",
}

def month_id(dt: datetime) -> int:
    return dt.year * 100 + dt.month   # YYYYMM (api key nur monthly)

def main():
    data = requests.get(URL, timeout=30).json()

    times = data["hourly"]["time"]
    temps = data["hourly"]["temperature_2m"]

    # 1) Monat initialisieren
    first_dt = datetime.fromisoformat(times[0])
    yyyymm = month_id(first_dt)
    month_date = first_dt.replace(day=1).date()

    # 2) Monats Aggregation
    avg_temp = float(mean(temps))
    min_temp = float(min(temps))
    max_temp = float(max(temps))

    # 3) Insert/Upsert in Postgres
    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn.cursor() as cur:
        # date_dim upsert (Monat als Datum = 1. Tag)
        cur.execute("""
            INSERT INTO date_dim (dateID, date, year, month, day)
            VALUES (%s, %s, %s, %s, 1)
            ON CONFLICT (dateID) DO UPDATE
            SET date = EXCLUDED.date,
                year = EXCLUDED.year,
                month = EXCLUDED.month,
                day = EXCLUDED.day;
        """, (yyyymm, month_date, first_dt.year, first_dt.month))

        # weather_dim upsert (weatherID = dateID = YYYYMM)
        cur.execute("""
            INSERT INTO weather_dim (weatherID, dateID, avgTemp, minTemp, maxTemp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (weatherID) DO UPDATE
            SET dateID  = EXCLUDED.dateID,
                avgTemp = EXCLUDED.avgTemp,
                minTemp = EXCLUDED.minTemp,
                maxTemp = EXCLUDED.maxTemp;
        """, (yyyymm, yyyymm, avg_temp, min_temp, max_temp))

    conn.commit()
    conn.close()

    print(" Monatswetter geladen:", yyyymm, avg_temp, min_temp, max_temp)

if __name__ == "__main__":
    main()
