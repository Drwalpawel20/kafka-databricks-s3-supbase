# Słownik danych

| Tabela | Klucz | Opis | Kolumny wymagane przez walidację |
|---|---|---|---|
| customers | customer_id | Klienci | customer_id |
| orders | order_id | Zamówienia | order_id, customer_id |
| order_items | order_id, product_id | Pozycje zamówień | order_id, product_id |
| products | product_id | Katalog produktów | product_id |
| payments | payment_id | Płatności | payment_id, order_id |

Lista wymaganych kolumn jest w `ingestion/config.py` (`REQUIRED_COLUMNS`). Pełny opis pól uzupełnij według swojego schematu w Supabase.
