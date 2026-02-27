#!/usr/bin/env python3
"""air_quality_fetcher.py

Cyclically pulls PM10 and PM2.5 measurements from the public GIOŚ REST API,
normalises the readings to a uniform JSON schema and stores each fetch in a
timestamped file (or another sink of your choice).  Designed to run either as
stand-alone script or inside a container / VM.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger

# nowe od ignacego
from kafka_producer import send_to_kafka


GIOS_API_ROOT = "https://api.gios.gov.pl/pjp-api/v1/rest"
# GIOŚ teraz kategorycznie wymaga tego nagłówka:
GIOS_HEADERS = {"Accept": "application/ld+json"}

###############################################################################
# Helper functions – raw HTTP layer
###############################################################################

def get_station_sensors(station_id: int) -> List[dict]:
    """Return all sensors attached to a given station."""
    url = f"{GIOS_API_ROOT}/station/sensors/{station_id}"
    r = requests.get(url, headers=GIOS_HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get("Lista stanowisk pomiarowych dla podanej stacji", [])


def get_sensor_values(sensor_id: int) -> List[dict]:
    """Return historic measurements for a sensor (may include None values)."""
    # Używamy size=500, żeby pobrać maksymalną możliwą paczkę danych historycznych
    url = f"{GIOS_API_ROOT}/data/getData/{sensor_id}?size=500"
    r = requests.get(url, headers=GIOS_HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get("Lista danych pomiarowych", [])

###############################################################################
# Normalisation – unify heterogeneous payloads
###############################################################################

def normalise_gios(values: List[dict], station_id: int, param_code: str) -> List[Dict]:
    """Convert GIOŚ payload to a flat, uniform schema suitable for Kafka/DB."""
    out: List[Dict] = []
    for v in values:
        val = v.get("Wartość")
        if val is None:  # skip missing rows
            continue
            
        out.append(
            {
                "source": "GIOS",
                "station_id": station_id,
                "param": param_code,  # Zawsze będzie PM10 lub PM25 (bez kropki, by nie psuć Kafki)
                "value": val,
                "unit": "µg/m³",
                "timestamp": v.get("Data"),  # original measurement time (ISO)
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return out

###############################################################################
# Single poll cycle
###############################################################################

def fetch_once(stations: List[int]) -> List[Dict]:
    """Pull fresh data for all configured stations and return normalised list."""
    records: List[Dict] = []

    for sid in stations:
        try:
            sensors = get_station_sensors(sid)
        except Exception as err:
            logger.error("Station {sid}: failed to load sensors – {err}", sid=sid, err=err)
            continue

        for sensor in sensors:
            raw_code = sensor.get("Wskaźnik - kod")
            if not raw_code:
                continue
                
            # Normalizacja "PM2.5" do "PM25", żeby zachować spójność z dawnym formatem dla Kafki
            code = "PM25" if raw_code == "PM2.5" else raw_code
            
            if code not in ("PM10", "PM25"):
                continue  # skip other pollutants

            sensor_id = sensor.get("Identyfikator stanowiska")
            if not sensor_id:
                continue

            try:
                values = get_sensor_values(sensor_id)
            except Exception as err:
                logger.error("Sensor {s}: failed to load values – {err}", s=sensor_id, err=err)
                continue

            records.extend(normalise_gios(values, sid, code))

    return records

###############################################################################
# Sink – persist to JSON file (can be swapped for Kafka, DB, etc.)
###############################################################################

def persist(records: List[Dict], out_dir: Path) -> None:
    if not records:
        logger.warning("No valid records fetched this cycle.")
        return

    # Wysyłka do Kafka
    try:
        send_to_kafka(records)
    except Exception as e:
        logger.error("Failed to send records to Kafka: {}", e)

    # (opcjonalnie także zapis lokalny)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"gios_{ts}.json"

    try:
        with path.open("w", encoding="utf-8") as fh:
            json.dump(records, fh, ensure_ascii=False, indent=2)
        logger.info("Persisted {n} records → {file}", n=len(records), file=path)
    except Exception as e:
        logger.error("Failed to write local JSON backup: {}", e)

###############################################################################
# Entry-point – scheduler loop
###############################################################################

def main() -> None:
    stations = [int(s) for s in os.getenv("GIOS_STATION_IDS", "400,401").split(",")]
    interval = int(os.getenv("FETCH_INTERVAL_MIN", "10"))
    out_dir = Path(os.getenv("OUTPUT_DIR", "./data"))

    # Loguru config – colourful stderr + rolling log file
    logger.remove()
    logger.add(
        "fetcher.log",
        rotation="10 MB",
        retention="14 days",
        backtrace=True,
        diagnose=True,
        level="INFO",
        enqueue=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level="INFO",
    )

    logger.info("📡 Starting GIOŚ fetcher – stations={}", stations)
    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        lambda: persist(fetch_once(stations), out_dir),
        trigger="interval",
        minutes=interval,
        next_run_time=datetime.now(timezone.utc),
        max_instances=1,
        coalesce=True,
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Fetch loop stopped – goodbye!")


if __name__ == "__main__":
    main()