# Mini Data Warehouse – Crime & Weather Analysis

## Projektübersicht

Dieses Projekt implementiert ein kleines Data Warehouse (DWH) für die Analyse von:

-  Kriminalitätsdaten (UK Police API)
-  Wetterdaten (Open-Meteo API)

Ziel ist es, mögliche Zusammenhänge zwischen **monatlicher Kriminalität** und **aggregierten Wetterkennzahlen** .

---

##  Projekt Ideen

### Ausgangslage
- Die Police API liefert **nur monatsbasierte Daten (YYYY-MM)**.
- Wetterdaten liegen **stündlich bzw. täglich** vor.

Späzifische Analysen (Wochenende, Tageszeit, Abend/Nacht) sind daher nicht möglich.

### Lösung
- Gemeinsame Analyseebene: **Monat**
- Wetterdaten werden auf Monatsbasis aggregiert
- Wetter wird als **zeitlicher Kontext** modelliert, nicht als Ereignis

---


---

## ETL-Prozess

Der ETL wird manuell über Python-Skripte ausgeführt:

- `weather_etl.py` – lädt & aggregiert Wetterdaten
- `police_etl.py` – lädt Crime Daten
- `run.py` 

---

## Installation & Setup
### SQL Dump intallieren

### DB erstellen
```bash
createdb -U postgres minidwh
```

### Dump importieren
```bash
psql -U postgres -d minidwh -f minidwh-dump.sql
```

---
### Virtuelle Umgebung erstellen
```bash

python -m venv .venv
```
# Umgebung aktivieren (Windows)
```bash
.venv\Scripts\activate
```
# Umgebung aktivieren (macOS/Linux)
```bash
source .venv/bin/activate
```
### requirements installieren
```bash
pip install -r requirements.txt
```
---
### Konfig erstellen (.env)
## Wichtig: Die .env darf nicht in einem Unterordner liegen, da sie sonst vom Skript nicht korrekt geladen wird.
- **Die `.env` muss unter `DWH/.env` erstellt werden und unten dargestellte Beispiel in `.env` hinzufügen**
```bash
PGHOST=
PGPORT=
PGDATABASE=minidwh
PGUSER=
PGPASSWORD=

#coordinat für Oxford und datum Zeitraum der Ereignisse
Month=2025-12  #Ziel Monat und Jahr
DEFAULT_LAT=51.758204 
DEFAULT_LNG=-1.255826
```
---
## TIPS / Hinweise:

- MONTH im Format YYYY-MM (z. B. 2025-11)

- Die Koordinaten müssen sich innerhalb Großbritanniens befinden

- Koordinaten können hier ermittelt werden:

```bash
https://www.gps-coordinates.net/
```
---
### ETL Prozess starten
## Um verschiedene Monate importieren zu können, muss das gewünschte Datum (Format YYYY-MM) in der .env-Datei angepasst werden. Anschließend muss der ETL-Prozess manuell erneut gestartet werden.
```bash
python run.py
```
---
