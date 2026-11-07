# E-commerce Data Platform

Projekt portfolio pokazujący integrację: **Supabase → Airflow → S3 → Databricks (Silver/Gold)** oraz równoległy strumień **S3 → SQS → Kafka → bronze**, z monitoringiem w Prometheus i Grafanie.

## Stack
- **Orkiestracja:** Apache Airflow 3.3 (CeleryExecutor, Redis, Postgres)
- **Źródło:** Supabase (paginacja, walidacja liczby wierszy i schematu)
- **Storage:** AWS S3, SQS
- **Streaming:** Kafka (producer, consumer, deduplikacja)
- **Przetwarzanie:** Databricks (Delta, PySpark). Joby działają w workspace Databricks, a ich id podaje się w `.env`.
- **Monitoring:** Prometheus, Grafana, kafka-exporter
- **Testy:** pytest

## Uruchomienie
1. Skopiuj `.env` do `.env` i uzupełnij wartości. Nie commituj `.env`.
2. Utwórz katalog danych, który Airflow i konsument zapisują na wolumenie (Linux/WSL):
   ```bash
   mkdir -p data airflow/logs && sudo chown -R 50000:0 data airflow/logs
   ```
3. Utwórz kolejkę SQS i powiadomienia S3 (jednorazowo):
   ```bash
   pip install -r requirements-dev.txt
   python scripts/setup_s3_events.py
   ```
4. Uruchom stack:
   ```bash
   docker compose up -d --build
   ```
5. Panele:
   - Airflow: http://localhost:8080 (login `admin`, hasło z `AIRFLOW_ADMIN_PASSWORD`, domyślnie `admin`)
   - Kafka UI: http://localhost:8085
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000

## Testy
```bash
pip install -r requirements-dev.txt
pytest -q
```

## Decyzje projektowe
- **Atomowy zapis parquet** (plik tymczasowy + `replace`), żeby niedokończony plik nigdy nie trafił dalej.
- **Walidacja przed wysyłką:** liczba wierszy względem Supabase, schemat, pusta tabela.
- **Dwie wersje w S3:** `archive/` z historią i `raw/` z najnowszą wersją. Listener reaguje tylko na `raw/orders`.
- **At-least-once + idempotencja:** `event_id` jest deterministyczny, a konsument go deduplikuje.
- **Commit offsetów po zapisie**, nie przed.
- **Id jobów Databricks w `.env`:** brak id przerywa parsowanie DAG-a, zamiast cichego uruchomienia joba 0.

Szczegóły: `docs/architecture.md`, `docs/failure_scenarios.md`, `databricks/README.md`.

## Ograniczenia (świadomie)
- Kafka przy tym wolumenie jest przerostem formy nad treścią. Projekt pokazuje umiejętność integracji.
- Deduplikacja w bronze nie jest w pełni transakcyjna. Duplikaty po awarii rozwiązuje warstwa Silver.
- Dashboard Grafany nie jest dołączony. Można go dodać jako JSON w `monitoring/grafana/provisioning/dashboards/`.
