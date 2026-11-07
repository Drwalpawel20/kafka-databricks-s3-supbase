from ingestion.supabase.extract import fetch_all_rows


class _Result:
    def __init__(self, data, count=None):
        self.data = data
        self.count = count


class _Query:
    """Atrapa zapytania Supabase: obsługuje select/order/range/execute."""

    def __init__(self, rows):
        self._rows = rows
        self._want_count = False
        self._range = (0, 999)

    def select(self, columns, count=None):
        self._want_count = count is not None
        return self

    def order(self, column):
        return self

    def range(self, start, end):
        self._range = (start, end)
        return self

    def execute(self):
        start, end = self._range
        count = len(self._rows) if self._want_count else None
        return _Result(self._rows[start:end + 1], count)


class _FakeClient:
    def __init__(self, rows):
        self._rows = rows

    def table(self, name):
        return _Query(self._rows)


def test_fetch_all_rows_paginates_until_last_page():
    rows = [{"order_id": i} for i in range(2500)]
    fetched, expected = fetch_all_rows(_FakeClient(rows), "orders", page_size=1000)

    assert len(fetched) == 2500
    assert expected == 2500
    assert fetched[-1]["order_id"] == 2499


def test_fetch_all_rows_handles_exact_page_boundary():
    rows = [{"order_id": i} for i in range(2000)]
    fetched, _ = fetch_all_rows(_FakeClient(rows), "orders", page_size=1000)

    assert len(fetched) == 2000