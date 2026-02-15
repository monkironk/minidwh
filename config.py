import os
from dotenv import load_dotenv

load_dotenv()

PG = {
    "host": os.environ["PGHOST"],
    "port": int(os.environ["PGPORT"]),
    "dbname": os.environ["PGDATABASE"],
    "user": os.environ["PGUSER"],
    "password": os.environ["PGPASSWORD"],
}

#Coordinate und Monat der Ereignisse
MONTH = os.environ["MONTH"]
DEFAULT_LAT = float(os.environ["DEFAULT_LAT"])
DEFAULT_LNG = float(os.environ["DEFAULT_LNG"])

