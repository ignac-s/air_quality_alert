import json
import logging
from datetime import datetime
from collections import OrderedDict
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BROKER_URL = 'localhost:9092'
INPUT_TOPIC_NAME = 'air_quality_processed'
ALERT_FILE_PATH = 'alerts.log'
ENABLE_FILE_LOGGING = True
CONSUMER_GROUP_ID_ALERTS = 'air-quality-alert-detector-group'

THRESHOLDS = {
    "PM2.5": {"inform": 40.0, "alarm": 60.0},
    "PM10": {"inform": 30.0, "alarm": 40.0},
}

ALREADY_ALERTED_EVENTS = OrderedDict()
MAX_ALERTED_EVENTS_SIZE = 200

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

logging.getLogger('kafka').setLevel(logging.INFO)

alert_logger = logging.getLogger('AlertSystem')
alert_logger.setLevel(logging.WARNING)
alert_logger.propagate = False

console_handler_alerts = logging.StreamHandler()
console_handler_alerts.setFormatter(logging.Formatter('%(asctime)s | POWIETRZE | %(message)s'))
alert_logger.addHandler(console_handler_alerts)

if ENABLE_FILE_LOGGING:
    try:
        file_handler_alerts = logging.FileHandler(ALERT_FILE_PATH, mode='a')
        file_handler_alerts.setFormatter(logging.Formatter('%(asctime)s | POWIETRZE | %(message)s'))
        alert_logger.addHandler(file_handler_alerts)
        logging.info(f"Alerty będą zapisywane do pliku: {ALERT_FILE_PATH}")
    except Exception as e:
        logging.error(f"Nie udało się włączyć zapisu alertów do pliku ({ALERT_FILE_PATH}): {e}")
        ENABLE_FILE_LOGGING = False


def generate_alert_message(processed_data: dict, pollutant: str, value: float, threshold: float, alert_level_name: str) -> str:
    station_id = processed_data.get("station_id", "brak danych")
    measurement_time = processed_data.get("measurement_time_utc", "brak danych")
    source = processed_data.get("source", "nieznane")

    message = (
        f"{alert_level_name.upper()}: Wysoki poziom {pollutant} na stacji {station_id} (źródło: {source})! "
        f"Odczyt: {value:.2f} µg/m³ (próg {alert_level_name.lower()}: {threshold:.2f} µg/m³). "
        f"Czas pomiaru UTC: {measurement_time}"
    )
    return message


def check_thresholds_and_alert(data_point: dict):
    pollutant = data_point.get("param")
    value_str = data_point.get("value")
    station_id = data_point.get("station_id")
    measurement_time_utc = data_point.get("measurement_time_utc")

    if not all([pollutant, value_str is not None, station_id is not None, measurement_time_utc]):
        logging.warning(f"Otrzymano niekompletne dane do utworzenia ID zdarzenia lub pomiaru, pomijam. Dane: {data_point}")
        return

    event_id = (station_id, pollutant, measurement_time_utc)

    if event_id in ALREADY_ALERTED_EVENTS:
        logging.debug(f"Alert dla zdarzenia {event_id} (rozmiar historii: {len(ALREADY_ALERTED_EVENTS)}) został już wcześniej wygenerowany. Pomijam.")
        return

    try:
        value = float(value_str)
    except (ValueError, TypeError):
        logging.warning(f"Pominięto dane dla {pollutant} (stacja: {station_id}, czas: {measurement_time_utc}): "
                        f"nieprawidłowa wartość '{value_str}'. Dane: {data_point}")
        return

    if pollutant in THRESHOLDS:
        pollutant_thresholds = THRESHOLDS[pollutant]
        alert_generated = False

        if value > pollutant_thresholds["alarm"]:
            alert_message = generate_alert_message(data_point, pollutant, value, pollutant_thresholds["alarm"], "Poziom alarmowy")
            alert_logger.warning(alert_message)
            alert_generated = True

        elif value > pollutant_thresholds["inform"]:
            alert_message = generate_alert_message(data_point, pollutant, value, pollutant_thresholds["inform"], "Poziom informowania")
            alert_logger.warning(alert_message)
            alert_generated = True

        if alert_generated:
            # Ręczny zapis z aktualnym timestampem, widoczny natychmiast
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            try:
                with open(ALERT_FILE_PATH, "a", encoding="utf-8") as f:
                    f.write(f"{now} | POWIETRZE | {alert_message}\n")
                    f.flush()
            except Exception as e:
                logging.error(f"Nie udało się ręcznie zapisać alertu do pliku: {e}")

            ALREADY_ALERTED_EVENTS[event_id] = True
            logging.info(f"Dodano zdarzenie {event_id} do listy zaalertowanych (rozmiar: {len(ALREADY_ALERTED_EVENTS)}).")

            if len(ALREADY_ALERTED_EVENTS) > MAX_ALERTED_EVENTS_SIZE:
                oldest_event_id, _ = ALREADY_ALERTED_EVENTS.popitem(last=False)
                logging.info(f"Przekroczono MAX_ALERTED_EVENTS_SIZE ({MAX_ALERTED_EVENTS_SIZE}). Usunięto najstarsze zdarzenie: {oldest_event_id}")


def main():
    consumer = None
    logging.info(f"Uruchamiam system alertów. Łączenie z Kafką ({KAFKA_BROKER_URL}, temat: {INPUT_TOPIC_NAME})...")
    logging.info(f"Historia alertów będzie ograniczona do {MAX_ALERTED_EVENTS_SIZE} ostatnich unikalnych zdarzeń.")
    try:
        consumer = KafkaConsumer(
            INPUT_TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER_URL],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=CONSUMER_GROUP_ID_ALERTS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        logging.info(f"Połączono. Nasłuchuję na '{INPUT_TOPIC_NAME}' w poszukiwaniu przekroczeń norm.")

        while True:
            for message in consumer:
                if message and message.value:
                    data_point = message.value
                    logging.debug(f"Odebrano przetworzony rekord: {data_point}")
                    check_thresholds_and_alert(data_point)

    except KafkaError as e:
        logging.critical(f"Krytyczny błąd Kafki - nie mogę przetwarzać danych! {e}")
    except json.JSONDecodeError as e:
        msg_val_str = "Niedostępna"
        if 'message' in locals() and hasattr(message, 'value'):
            try:
                msg_val_str = message.value.decode('utf-8', errors='replace') if isinstance(message.value, bytes) else str(message.value)
            except Exception:
                msg_val_str = "Nie udało się zdekodować/odczytać wartości wiadomości"
        logging.error(f"Błąd odczytu danych (JSON): {e}. Treść wiadomości (lub jej fragment): {msg_val_str[:200]}")
    except KeyboardInterrupt:
        logging.info("System alertów zatrzymany przez użytkownika.")
    except Exception as e:
        logging.error(f"Niespodziewany problem w systemie alertów: {e}", exc_info=True)
    finally:
        if consumer:
            consumer.close()
            logging.info("Połączenie z Kafką zamknięte.")
        logging.info("System alertów zakończył działanie.")


if __name__ == "__main__":
    main()

