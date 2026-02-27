# air_quality_processor.py
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

# Konfiguracja
KAFKA_SERVER = "localhost:9092"
RAW_TOPIC = "air_quality_raw"
PROCESSED_TOPIC = "air_quality_processed"
CONSUMER_GROUP_ID = "air-quality-processor-group" # Ważne dla Kafki

# Loguru config
logger.remove()
logger.add(
    "processor.log",
    rotation="10 MB",
    retention="14 days",
    backtrace=True,
    diagnose=True,
    level="INFO",
    enqueue=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}:{function}:{line}</cyan> - <level>{message}</level>",
)
logger.add(
    lambda msg: print(msg, end=""),
    level="INFO",
)

# Funkcje pomocnicze
def validate_and_transform_record(raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Waliduje surowy rekord i transformuje go do przetworzonego formatu.
    Agregacja na minutowe średnie oznacza przypisanie wartości godzinowej
    do konkretnej minuty (np. pierwszej minuty godziny pomiaru)
    """
    try:
        # Walidacja podstawowych pól
        required_keys = {"source", "station_id", "param", "value", "unit", "timestamp", "fetched_at"}
        if not required_keys.issubset(raw_record.keys()):
            logger.warning(f"Rekord nie zawiera wszystkich wymaganych kluczy: {raw_record}")
            return None

        if raw_record["value"] is None:
            logger.warning(f"Pusta wartość 'value' w rekordzie: {raw_record}")
            return None

        if not isinstance(raw_record["value"], (int, float)):
            logger.warning(f"Niepoprawny typ dla 'value': {type(raw_record['value'])} w {raw_record}")
            return None

        if raw_record["param"] not in ("PM10", "PM2.5"):
            logger.debug(f"Ignorowanie parametru innego niż PM10/PM2.5: {raw_record['param']}")
            return None

        # Konwersja i normalizacja timestampu
        # Oryginalny timestamp może mieć spację zamiast 'T'
        original_timestamp_str = raw_record["timestamp"]
        try:
            # Dla uproszczenia, jeśli nie ma 'T', zakładamy że to format 'YYYY-MM-DD HH:MM:SS'
            if 'T' not in original_timestamp_str and ' ' in original_timestamp_str:
                 dt_original = datetime.strptime(original_timestamp_str, "%Y-%m-%d %H:%M:%S")
            else:
                 dt_original = datetime.fromisoformat(original_timestamp_str)

            # Jeśli nie ma strefy czasowej, zakładamy UTC
            if dt_original.tzinfo is None:
                dt_original = dt_original.replace(tzinfo=timezone.utc)
            else: # Normalizuj do UTC
                dt_original = dt_original.astimezone(timezone.utc)

        except ValueError as e:
            logger.error(f"Nie udało się sparsować 'timestamp': {original_timestamp_str} - {e}. Rekord: {raw_record}")
            return None

        # Agregacja na minutowe średnie:
        # Dla danych godzinowych, przypisujemy odczyt do pierwszej minuty tej godziny.
        # Timestamp będzie miał zerowe sekundy i mikrosekundy.
        minute_timestamp = dt_original.replace(second=0, microsecond=0)

        processed_record = {
            "source": raw_record["source"],
            "station_id": int(raw_record["station_id"]),
            "param": raw_record["param"],
            "value": float(raw_record["value"]),
            "unit": raw_record["unit"],
            "measurement_time_utc": minute_timestamp.isoformat(), # Ujednolicony timestamp
            "original_timestamp": original_timestamp_str,
            "fetched_at_source_utc": raw_record["fetched_at"], # Czas pobrania przez fetcher
            "processed_at_utc": datetime.now(timezone.utc).isoformat() # Czas przetworzenia przez ten skrypt
        }
        return processed_record

    except Exception as e:
        logger.error(f"Nieoczekiwany błąd podczas przetwarzania rekordu {raw_record}: {e}")
        return None


# Główna logika
def main():
    logger.info(f" uruchamiam konsumenta dla tematu '{RAW_TOPIC}'...")
    try:
        consumer = KafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',  # Czytaj od początku, jeśli nowy konsument
            group_id=CONSUMER_GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode('utf-8', 'ignore')) # Ignoruj błędy dekodowania
        )
    except KafkaError as e:
        logger.critical(f"Nie udało się połączyć konsumenta z Kafką: {e}")
        return

    logger.info(f" uruchamiam producenta dla tematu '{PROCESSED_TOPIC}'...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except KafkaError as e:
        logger.critical(f"Nie udało się połączyć producenta z Kafką: {e}")
        consumer.close() # Zamknij konsumenta jeśli producent padł
        return

    logger.info("Rozpoczynam konsumpcję i przetwarzanie wiadomości...")
    try:
        for message in consumer:
            logger.debug(f"Odebrano surowy rekord z offset {message.offset}: {message.value}")
            raw_record = message.value

            processed_record = validate_and_transform_record(raw_record)

            if processed_record:
                try:
                    producer.send(PROCESSED_TOPIC, value=processed_record)
                    logger.info(f"Wysłano przetworzony rekord na temat '{PROCESSED_TOPIC}': {processed_record}")
                except KafkaError as e:
                    logger.error(f"Nie udało się wysłać przetworzonego rekordu do Kafki: {e}. Rekord: {processed_record}")
            else:
                logger.warning(f"Rekord został odrzucony lub zignorowany: {raw_record}")

    except KeyboardInterrupt:
        logger.info("Przerwano przez użytkownika (KeyboardInterrupt). Zamykanie...")
    except Exception as e:
        logger.critical(f"Krytyczny błąd w głównej pętli: {e}", exc_info=True)
    finally:
        logger.info("Zamykanie konsumenta i producenta...")
        if 'consumer' in locals() and consumer: # Sprawdź czy consumer istnieje
            consumer.close()
        if 'producer' in locals() and producer: # Sprawdź czy producer istnieje
            producer.flush() # Upewnij się, że wszystkie wiadomości zostały wysłane
            producer.close()
        logger.info("Zasoby Kafki zostały zwolnione")

if __name__ == "__main__":
    main()