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

### Virtuelle Umgebung erstellen
```bash
python -m venv .venv

.venv\Scripts\activate

pip install -r requirements.txt

python run.py

