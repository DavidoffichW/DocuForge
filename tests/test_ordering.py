import pytest

from core.ordering import sort_strings, sort_records


def test_sort_strings_canonical():
    items = ["b", "a", "c"]
    assert sort_strings(items) == ["a", "b", "c"]


def test_sort_records_by_key():
    records = [
        {"id": 2},
        {"id": 1},
        {"id": 3},
    ]

    sorted_records = sort_records(records, ["id"])
    assert [r["id"] for r in sorted_records] == [1, 2, 3]


def test_sort_records_missing_key_raises():
    records = [{"a": 1}]
    with pytest.raises(ValueError):
        sort_records(records, ["id"])