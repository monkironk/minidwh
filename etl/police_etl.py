import requests
from db import get_conn

def month_to_dateid(month: str) -> int:
    y, m = month.split("-")
    return int(y) * 100 + int(m)

def month_first_date(month: str) -> str:
    return f"{month}-01"

def stable_int_id(*parts) -> int:
    
    return abs(hash(parts)) % 2000000000

def load_police_month(lat: float, lng: float, month: str, set_weather_id: bool = False):
    url = f"https://data.police.uk/api/crimes-street/all-crime?date={month}&lat={lat}&lng={lng}"
    crimes = requests.get(url, timeout=30).json()

    with get_conn() as conn:
        with conn.cursor() as cur:
            for c in crimes:
                crime_id = int(c["id"])           
                month_str = c["month"]           
                date_id = month_to_dateid(month_str)
                first_date = month_first_date(month_str)

                year = int(month_str.split("-")[0])
                mon = int(month_str.split("-")[1])

                # date_dim
                cur.execute("""
                    INSERT INTO date_dim (dateID, date, year, month, day)
                    VALUES (%s, %s::date, %s, %s, 1)
                    ON CONFLICT (dateID) DO NOTHING;
                """, (date_id, first_date, year, mon))

                # category_dim (unique categoryName)
                category_name = c["category"]
                category_id = stable_int_id("cat", category_name)

                cur.execute("""
                    INSERT INTO category_dim (categoryID, categoryName)
                    VALUES (%s, %s)
                    ON CONFLICT (categoryID) DO NOTHING;
                """, (category_id, category_name))

                # location_dim (street id ist quassi die location)
                loc = c["location"]
                street = loc.get("street") or {}
                street_id = street.get("id")
                street_name = street.get("name")

                location_id = int(street_id) if street_id is not None else stable_int_id("loc", street_name)

                cur.execute("""
                    INSERT INTO location_dim (locationID, streetID, streetName)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (locationID) DO UPDATE
                    SET streetID = EXCLUDED.streetID,
                        streetName = EXCLUDED.streetName;
                """, (location_id, street_id, street_name))

                # status_dim (nullable)
                outcome = c.get("outcome_status")
                status_id = None
                if outcome:
                    status_text = outcome.get("category")
                    status_month = outcome.get("date")      # "YYYY-MM"
                    status_date = month_first_date(status_month) if status_month else None
                    status_id = stable_int_id("status", status_text, status_month)

                    cur.execute("""
                        INSERT INTO status_dim (statusID, statusArten, statusDate)
                        VALUES (%s, %s, %s::date)
                        ON CONFLICT (statusID) DO UPDATE
                        SET statusArten = EXCLUDED.statusArten,
                            statusDate  = EXCLUDED.statusDate;
                    """, (status_id, status_text, status_date))

                # ort
                latitude = float(loc["latitude"])
                longitude = float(loc["longitude"])

                weather_id = date_id if set_weather_id else None

                cur.execute("""
                    INSERT INTO crimes_fakten
                      (crimeID, dateID, locationID, weatherID, categoryID, statusID, latitude, longitude)
                    VALUES
                      (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (crimeID) DO NOTHING;
                """, (crime_id, date_id, location_id, weather_id, category_id, status_id, latitude, longitude))

    return len(crimes)
