#!/bin/bash

echo "▶️  Uruchamiam Docker..."
docker-compose up -d

echo "⏳ Czekam 5 sekund na start kontenerów..."
sleep 5

echo "▶️  Uruchamiam skrypty wewnątrz kontenera 'backend_api'..."

docker exec -d backend_api sh -c "cd /app && python air_quality_fetcher.py > fetcher.log 2>&1"
docker exec -d backend_api sh -c "cd /app && python air_quality_processor.py > processor.log 2>&1"
docker exec -d backend_api sh -c "cd /app && python alert_detector.py > detector.log 2>&1"

echo "✅ Wszystko uruchomione! Otwórz http://localhost:8000/docs"
