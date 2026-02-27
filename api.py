from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, List, Dict
from pathlib import Path
import json

app = FastAPI()

DATA_DIR = Path("data")
ALERTS_LOG_PATH = Path("alerts.log")  # ← plik z alertami

#  Model danych jakości powietrza
class AirQualityEntry(BaseModel):
    source: str
    station_id: int
    param: str
    value: float
    unit: str
    timestamp: str
    fetched_at: Optional[str]

# Model danych alertu
class AlertEntry(BaseModel):
    timestamp: str
    level: str
    message: str

#  Wczytywanie danych z plików JSON
def load_all_data() -> List[Dict]:
    all_data = []
    for file_path in DATA_DIR.glob("*.json"):
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as file:
                content = json.load(file)
                if isinstance(content, list):
                    for entry in content:
                        if "unit" in entry and "Â" in entry["unit"]:
                            entry["unit"] = "µg/m³"
                    all_data.extend(content)
        except Exception as e:
            print(f"Problem z plikiem {file_path}: {e}")
    return all_data

#  Wczytywanie alertów z logu 
def load_alerts(limit: int = 5) -> List[AlertEntry]:
    try:
        with open(ALERTS_LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return []

    parsed_alerts = []

    for line in reversed(lines):
        try:
            parts = line.split(" | ")
            if len(parts) < 3:
                continue
            timestamp = parts[0].strip()
            level, _, message = parts[2].partition(":")
            parsed_alerts.append(AlertEntry(
                timestamp=timestamp,
                level=level.strip(),
                message=message.strip()
            ))
        except Exception:
            continue

        if len(parsed_alerts) >= limit:
            break

    return parsed_alerts

#  Endpoint: najnowszy pomiar
@app.get("/air_quality/current", response_model=AirQualityEntry)
def get_latest_entry():
    data = load_all_data()
    if not data:
        return {"error": "No data available"}
    latest = sorted(data, key=lambda x: x["timestamp"], reverse=True)
    return latest[0]

#  Endpoint: 
@app.get("/air_quality/latest_by_station", response_model=List[AirQualityEntry])
def get_latest_by_station():
    data = load_all_data()
    if not data:
        return {"error": "No data available"}

    latest_per_station = {}
    for entry in sorted(data, key=lambda x: x["timestamp"], reverse=True):
        sid = entry["station_id"]
        if sid not in latest_per_station:
            latest_per_station[sid] = entry
    return list(latest_per_station.values())

#  Endpoint: 
@app.get("/alerts/latest", response_model=List[AlertEntry])
def get_latest_alerts(limit: int = 5):
    return load_alerts(limit)
