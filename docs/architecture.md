# Architektura

```mermaid
flowchart LR
    SB[(Supabase)] -->|extract + validation| AF[Airflow]
    AF -->|parquet| S3RAW[(S3 raw/ + archive/)]
    S3RAW -->|ObjectCreated| SQS[SQS]
    SQS --> L[sqs_listener]
    L --> K[[Kafka: orders]]
    K --> C[consumer + dedup]
    C --> BR[(bronze parquet)]
    AF -->|run job| DB1[Databricks Silver]
    DB1 --> DB2[Databricks Gold]
```

Dwa równoległe przepływy:
1. **Batch:** Airflow pobiera tabele raz dziennie, waliduje, wysyła do S3 i uruchamia joby Databricks (Silver → Gold).
2. **Streaming:** każdy nowy plik `raw/orders*.parquet` trafia przez SQS i Kafkę do warstwy bronze z deduplikacją.

Kafka jest tu do demonstracji integracji. Przy tak małym wolumenie sam batch byłby wystarczający.
