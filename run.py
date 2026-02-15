from config import DEFAULT_LAT, DEFAULT_LNG, MONTH
from etl.weather_etl import load_weather_month
from etl.police_etl import load_police_month
from etl.post_load import fill_weather_ids


def main():
    lat, lng = DEFAULT_LAT, DEFAULT_LNG
    month = MONTH  # "YYYY-MM"

    print(f" Location: lat={lat}, lng={lng}")
    print(f" Month: {month}")

    # 1) Weather (Monatsaggregation)
    weather = load_weather_month(lat, lng, month)
    print(" Wetter Daten wurden geladen:", weather)

    # 2) Police (monatliche Crimes)
    crimes = load_police_month(lat, lng, month, set_weather_id=True)
    print(f" Polizei Daten wurden geladen: {crimes}")

    # 3) falls irgendwo weatherID NULL blieb
    fill_weather_ids()
    print(" weatherID=dateID ")

    print(" ETL war erfolgreich.")


if __name__ == "__main__":
    main()
