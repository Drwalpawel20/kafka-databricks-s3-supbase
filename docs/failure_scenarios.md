# Scenariusze awarii

| Scenariusz | Co się dzieje | Mechanizm obrony |
|---|---|---|
| Supabase zwraca niepełne dane | Liczba wierszy ≠ `count` | `validate_row_count` przerywa task, plik nie idzie do S3 |
| Tabela bez wymaganych kolumn | Zmiana schematu w Supabase | `validate_schema` przerywa task przed uploadem |
| Upload do S3 przerwany lub niepełny | Plik częściowy w S3 | Porównanie rozmiaru `head_object`, błąd → retry Airflow |
| Listener padł po wysłaniu do Kafki, a przed usunięciem z SQS | Ten sam plik przetwarzany ponownie | Deterministyczny `event_id` + `DedupStore` w konsumencie |
| Konsument padł po zapisie bronze, a przed `mark_processed` | Duplikaty w bronze po restarcie | Znane ograniczenie: deduplikacja w Silver po `_event_id` |
| Kafka niedostępna | Listener nie wysyła, wiadomości zostają w SQS | Retry przez VisibilityTimeout, `acks=all` |
| Brak credentials AWS (np. stara nazwa `aws_access` w .env) | Listener i upload nie mają dostępu do AWS | Aliasy w `ingestion/config.py`; poprawne nazwy w `../.env` |
| Brak id jobu Databricks w .env | Błąd konfiguracji | `_job_id` przerywa parsowanie DAG-a z czytelnym komunikatem |
| Job Databricks nie działa | Gold nie jest odświeżany | Retry w Airflow, widoczny błąd taska `process_silver` / `process_gold` |
