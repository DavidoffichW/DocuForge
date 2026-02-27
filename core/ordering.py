from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence


def sort_strings(items: List[str]) -> List[str]:
    if not isinstance(items, list):
        raise ValueError("items must be a list")
    for x in items:
        if not isinstance(x, str):
            raise ValueError("all items must be strings")
    return sorted(items)


def sort_records(records: List[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("records must be a list")
    if not isinstance(keys, list) or not keys:
        raise ValueError("keys must be a non-empty list")
    for k in keys:
        if not isinstance(k, str) or not k.strip():
            raise ValueError("keys must be non-empty strings")

    for r in records:
        if not isinstance(r, dict):
            raise ValueError("each record must be a dict")
        for k in keys:
            if k not in r:
                raise ValueError("record missing required key")

    def _key(r: Dict[str, Any]):
        return tuple(r[k] for k in keys)

    return sorted(records, key=_key)


def sort_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(d, dict):
        raise ValueError("sort_dict requires dict")
    return {k: d[k] for k in sorted(d.keys())}


def sort_dict_recursive(d: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(d, dict):
        raise ValueError("sort_dict_recursive requires dict")
    out: Dict[str, Any] = {}
    for k in sorted(d.keys()):
        v = d[k]
        if isinstance(v, dict):
            out[k] = sort_dict_recursive(v)
        elif isinstance(v, list):
            out[k] = sort_list_recursive(v)
        else:
            out[k] = v
    return out


def sort_list_recursive(values: Iterable[Any]) -> List[Any]:
    result: List[Any] = []
    for v in values:
        if isinstance(v, dict):
            result.append(sort_dict_recursive(v))
        elif isinstance(v, list):
            result.append(sort_list_recursive(v))
        else:
            result.append(v)
    return result