from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple


def sort_strings(items: List[str]) -> List[str]:
    if any(not isinstance(x, str) for x in items):
        raise ValueError("sort_strings expects a list[str]")
    return sorted(items)


def canonicalize_keys(keys: List[str]) -> List[str]:
    return sort_strings(keys)


def sort_records(records: List[Dict[str, Any]], key_fields: List[str]) -> List[Dict[str, Any]]:
    if any(not isinstance(r, dict) for r in records):
        raise ValueError("sort_records expects a list[dict]")
    if any(not isinstance(k, str) or not k for k in key_fields):
        raise ValueError("key_fields must be a list of non-empty strings")

    def key_fn(r: Dict[str, Any]) -> Tuple[Any, ...]:
        for k in key_fields:
            if k not in r:
                raise ValueError(f"missing key field: {k}")
        return tuple(r[k] for k in key_fields)

    try:
        return sorted(records, key=key_fn)
    except TypeError as e:
        raise ValueError("key fields must be mutually comparable across records") from e


def sort_by_tuple(items: Sequence[Any], key_tuples: Sequence[Tuple[Any, ...]]) -> List[Any]:
    if len(items) != len(key_tuples):
        raise ValueError("items and key_tuples must have the same length")
    indexed = list(zip(key_tuples, items))
    try:
        indexed.sort(key=lambda x: x[0])
    except TypeError as e:
        raise ValueError("key_tuples must be comparable") from e
    return [item for _, item in indexed]