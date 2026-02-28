from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from core.ordering import sort_dict


MAX_EXPORT_ROWS = 50000


def _load_detection_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    params = payload.get("params")
    if not isinstance(params, dict):
        raise ValueError("params must be a dict")

    detection_bytes = params.get("table_detection_bytes")
    if not isinstance(detection_bytes, (bytes, bytearray)) or len(detection_bytes) == 0:
        raise ValueError("table_detection_bytes must be non-empty bytes")

    obj = json.loads(detection_bytes.decode("utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("table_detection payload must be JSON object")
    return obj


def _iter_tables(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    t = obj.get("tables")
    if not isinstance(t, list):
        raise ValueError("tables must be a list")
    out: List[Dict[str, Any]] = []
    for x in t:
        if isinstance(x, dict):
            out.append(x)
    return out


def _row_obj(row: List[Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for i, c in enumerate(row):
        k = f"c{str(i + 1).zfill(3)}"
        out[k] = "" if c is None else str(c)
    return out


def make_tables_export_jsonl_execution():
    def _exec(payload: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        obj = _load_detection_json(payload)

        tables = _iter_tables(obj)
        if len(tables) == 0:
            raise ValueError("no tables to export")

        lines: List[str] = []
        rows_written = 0

        for t in tables:
            grid = t.get("grid")
            if not isinstance(grid, list):
                raise ValueError("table grid must be list")
            if len(grid) > MAX_EXPORT_ROWS:
                raise ValueError("export row limit exceeded")

            for r in grid:
                if not isinstance(r, list):
                    raise ValueError("table row must be list")
                o = _row_obj(r)
                lines.append(json.dumps(o, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
                rows_written += 1

        data = ("\n".join(lines)).encode("utf-8")

        return (
            data,
            sort_dict(
                {
                    "artifact_kind": "bin",
                    "media_type": "application/jsonl",
                    "manifest": sort_dict(
                        {
                            "format": "jsonl",
                            "tables": len(tables),
                            "rows_written": rows_written,
                            "max_rows_per_table": MAX_EXPORT_ROWS,
                        }
                    ),
                }
            ),
        )

    return _exec