# Joby Databricks

Joby Silver i Gold są utrzymywane bezpośrednio w workspace Databricks.
Repozytorium zawiera tylko ich opis i wyzwalanie z Airflow (`DatabricksRunNowOperator`).

| Job | Zmienna w .env | Wejście | Wyjście |
|---|---|---|---|
| Silver | `DATABRICKS_SILVER_JOB_ID` | `s3://<bucket>/raw/<tabela>.parquet` | tabele Delta Silver |
| Gold | `DATABRICKS_GOLD_JOB_ID` | tabele Silver | tabele Delta Gold |

## Kontrakt z pipeline'em
- Airflow zapisuje zawsze najnowszą wersję każdej tabeli w `raw/<tabela>.parquet`.
- Historia pobrań jest w `archive/<tabela>/dt=<data>/`. Joby Databricks jej nie czytają.
- Silver startuje dopiero po udanym uploadzie wszystkich tabel (`uploaded >> silver >> gold` w DAG).

Kod jobów jest w workspace, a nie w tym repozytorium. Zmiany w logice rób w Databricks.
