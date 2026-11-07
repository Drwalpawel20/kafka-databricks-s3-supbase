from ingestion.kafka.deduplication import DedupStore
from ingestion.kafka.producer import make_event_id


def test_event_id_is_deterministic():
    assert make_event_id("b", "k", "e", 1) == make_event_id("b", "k", "e", 1)


def test_event_id_changes_with_index():
    assert make_event_id("b", "k", "e", 1) != make_event_id("b", "k", "e", 2)


def test_mark_and_check(tmp_path):
    store = DedupStore(str(tmp_path / "dedup.sqlite"))
    assert not store.is_processed("a")

    store.mark_processed(["a", "a", "b"])  # duplikat w jednym wywołaniu nie może wywalić błędu

    assert store.is_processed("a")
    assert store.is_processed("b")
    assert not store.is_processed("c")
    store.close()


def test_state_survives_reopen(tmp_path):
    path = str(tmp_path / "dedup.sqlite")
    store = DedupStore(path)
    store.mark_processed(["x"])
    store.close()

    reopened = DedupStore(path)
    assert reopened.is_processed("x")
    reopened.close()
