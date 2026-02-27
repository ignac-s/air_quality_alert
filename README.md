# Projekt Monitorowania Jakości Powietrza

## Opis Ogólny

Rozproszony system do cyklicznego pobierania, przetwarzania w czasie rzeczywistym oraz analizy danych o jakości powietrza. Architektura oparta na mikrousługach i strumieniowym przesyłaniu wiadomości pozwala na bieżące monitorowanie zanieczyszczeń (PM10, PM2.5) oraz generowanie automatycznych alertów o przekroczeniach norm.

Projekt wykorzystuje narzędzia takie jak Apache Kafka w roli brokera wiadomości oraz FastAPI do serwowania przetworzonych wyników i alertów poprzez interfejs REST API.

```mermaid
graph LR
    A[GIOŚ API] -- REST GET --> B(Fetcher Service)
    B -- JSON --> C{Kafka: air_quality_raw}
    C --> D(Processor Service)
    D -- Walidacja & Normalizacja --> E{Kafka: air_quality_processed}
    E --> F(Alert Detector)
    E --> G(FastAPI Server)
    F -- Logi Alertów --> H[Plik / Konsola]
    G -- REST API --> I[Użytkownik / Swagger UI]

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#ff9,stroke:#333,stroke-width:2px
    style E fill:#ff9,stroke:#333,stroke-width:2px
    style G fill:#bbf,stroke:#333,stroke-width:2px
```

Składa się z komponentów:
1.  **`air_quality_fetcher.py`**: Pobiera dane o jakości powietrza (PM10 i PM2.5) z publicznego API GIOŚ.
2.  **Apache Kafka**: Broker wiadomości używany do przesyłania danych między komponentami.
3.  **`air_quality_processor.py`**: Konsumuje surowe dane z Kafki, waliduje je, transformuje (np. normalizuje timestampy) i wysyła przetworzone dane na inny temat Kafki.

---

## Komponent 1: `air_quality_fetcher.py` (Pobieranie Danych)

**Opis:**
Skrypt `air_quality_fetcher.py` pobiera dane o jakości powietrza (PM10 i PM2.5) z publicznego API GIOŚ, cyklicznie (domyślnie co 10 minut).
Dane są normalizowane do formatu JSON, a następnie:
  - Wysyłane do Apache Kafka (topic: `air_quality_raw`).
  - (Opcjonalnie) Zapisywane lokalnie do plików JSON w katalogu `data/`.

Logi działania zapisywane są do pliku `fetcher.log`.

**Dane wejściowe:**
  - API GIOŚ (otwarte, bez autoryzacji)
  - Lista stacji i sensorów PM10/PM2.5

**Dane wyjściowe:**
  - JSON na temat Kafki: `air_quality_raw`
  - JSON zapisywany lokalnie w `./data/` (opcjonalnie)
  - Logi: `./fetcher.log`

**Konfiguracja (przez zmienne środowiskowe – opcjonalnie):**
  - `GIOS_STATION_IDS`: Lista ID stacji (domyślnie: "400,401")
  - `FETCH_INTERVAL_MIN`: Interwał pobierania w minutach (domyślnie: 10)
  - `OUTPUT_DIR`: Folder na pliki JSON (domyślnie: `./data`)

---

## Komponent 2: `air_quality_processor.py` (Przetwarzanie Danych)

**Opis:**
Skrypt `air_quality_processor.py` działa jako konsument Kafki dla tematu `air_quality_raw`.
Jego zadania to:
  - Odbieranie surowych danych o jakości powietrza.
  - Walidacja poprawności danych (struktura, typy, wartości).
  - Wstępna filtracja (np. ignorowanie pustych lub niekompletnych rekordów).
  - Transformacja danych, np. normalizacja timestampów pomiarów do formatu ISO 8601 UTC z dokładnością do minuty (np. `measurement_time_utc`).
  - Przesyłanie przetworzonych danych na nowy temat Kafki: `air_quality_processed`.

Logi działania zapisywane są do pliku `processor.log`.

**Dane wejściowe:**
  - JSON z tematu Kafki: `air_quality_raw`

**Dane wyjściowe:**
  - JSON na temat Kafki: `air_quality_processed`
  - Logi: `./processor.log`

**Konfiguracja (w kodzie):**
  - `KAFKA_SERVER`: Adres serwera Kafki (domyślnie: "localhost:9092")
  - `RAW_TOPIC`: Temat wejściowy (domyślnie: "air_quality_raw")
  - `PROCESSED_TOPIC`: Temat wyjściowy (domyślnie: "air_quality_processed")
  - `CONSUMER_GROUP_ID`: ID grupy konsumenta (domyślnie: "air-quality-processor-group")

---

## Wymagania Systemowe

  - Python 3.10 lub nowszy
  - Apache Kafka + Zookeeper (uruchamiane przez Docker Compose dostarczony w `docker-compose.yml`)
  - Docker i Docker Compose

## Instalacja Zależności

1.  Stwórz i aktywuj środowisko wirtualne (opcjonalne, ale zalecane):
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # Dla Linux/macOS
    # .venv\Scripts\activate   # Dla Windows
    ```

2.  Zainstaluj zależności Python:
    ```bash
    pip install -r requirements.txt
    ```

## Uruchomienie Systemu

1.  **Uruchom Apache Kafka i Zookeeper:**
    W głównym katalogu projektu, gdzie znajduje się plik `docker-compose.yml`, wykonaj:
    ```bash
    docker-compose up -d
    ```
    Poczekaj chwilę, aż kontenery się uruchomią. Możesz sprawdzić ich status przez `docker ps`.
    Upewnij się, że plik `docker-compose.yml` zawiera definicję tematów: `air_quality_raw` oraz `air_quality_processed`.

2.  **Uruchom `air_quality_fetcher.py` (Pobieranie Danych):**
    Otwórz nowy terminal (z aktywowanym środowiskiem wirtualnym, jeśli używasz) i uruchom:
    ```bash
    python air_quality_fetcher.py
    ```
    Skrypt zacznie pobierać dane i wysyłać je na temat `air_quality_raw`.

3.  **Uruchom `air_quality_processor.py` (Przetwarzanie Danych):**
    Otwórz kolejny nowy terminal (z aktywowanym środowiskiem wirtualnym) i uruchom:
    ```bash
    python air_quality_processor.py
    ```
    Skrypt zacznie nasłuchiwać na temacie `air_quality_raw`, przetwarzać dane i wysyłać je na temat `air_quality_processed`.

4. W nowym terminalu uruchom 
	python alert_detector.py

## Sprawdzanie Działania (Opcjonalnie)

Możesz obserwować wiadomości na tematach Kafki używając narzędzia `kafka-console-consumer` dostarczanego z Kafką.

*   **Aby zobaczyć surowe dane (z `air_quality_fetcher.py`):**
    ```bash
    docker exec -it kafka kafka-console-consumer \
      --bootstrap-server localhost:9092 \
      --topic air_quality_raw \
      --from-beginning
    ```

*   **Aby zobaczyć przetworzone dane (z `air_quality_processor.py`):**
    ```bash
    docker exec -it kafka kafka-console-consumer \
      --bootstrap-server localhost:9092 \
      --topic air_quality_processed \
      --from-beginning
    ```

## Wyłączanie Systemu

1.  **Zatrzymaj skrypty Pythona:**
    W terminalach, gdzie działają `air_quality_fetcher.py` i `air_quality_processor.py`, naciśnij `Ctrl+C`.

2.  **Zatrzymaj kontenery Docker (Kafka i Zookeeper):**
    W terminalu, w katalogu z plikiem `docker-compose.yml`, wykonaj:
    ```bash
    docker-compose down
    ```

3.  **Dezaktywuj środowisko wirtualne (jeśli było używane):**
    ```bash
    deactivate
    ```

## Dodatkowe Pliki

  - `docker-compose.yml`: Definicja usług Docker dla Kafka i Zookeeper.
  - `kafka_producer.py`: Moduł pomocniczy używany przez `air_quality_fetcher.py` do wysyłania danych do Kafki.
  - `requirements.txt`: Lista zależności Python.
  - `gios_20250508T111758Z.json`: Przykładowy plik z danymi generowanymi przez `air_quality_fetcher.py`.

