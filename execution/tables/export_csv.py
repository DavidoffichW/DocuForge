from __future__ import annotations

import csv
import io
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


def make_tables_export_csv_execution():
    def _exec(payload: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        obj = _load_detection_json(payload)
        params = payload["params"]

        include_header = params.get("include_header", False)
        header_row_index = params.get("header_row_index", None)

        if not isinstance(include_header, bool):
            raise ValueError("include_header must be bool")

        if header_row_index is not None and (not isinstance(header_row_index, int) or header_row_index < 1):
            raise ValueError("header_row_index must be int >= 1 or null")

        tables = _iter_tables(obj)
        if len(tables) == 0:
            raise ValueError("no tables to export")

        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")

        total_rows_written = 0

        for t in tables:
            grid = t.get("grid")
            if not isinstance(grid, list):
                raise ValueError("table grid must be list")
            rows: List[List[str]] = []
            for r in grid:
                if not isinstance(r, list):
                    raise ValueError("table row must be list")
                rows.append([str(c) if c is not None else "" for c in r])

            if len(rows) > MAX_EXPORT_ROWS:
                raise ValueError("export row limit exceeded")

            start_row = 0
            if include_header:
                if header_row_index is None:
                    raise ValueError("header_row_index required when include_header is true")
                idx = header_row_index - 1
                if idx < 0 or idx >= len(rows):
                    raise ValueError("header_row_index out of range")
                writer.writerow(rows[idx])
                start_row = idx + 1

            for r in rows[start_row:]:
                writer.writerow(r)

            total_rows_written += max(0, len(rows) - start_row)

        data = out.getvalue().encode("utf-8")

        return (
            data,
            sort_dict(
                {
                    "artifact_kind": "bin",
                    "media_type": "text/csv",
                    "manifest": sort_dict(
                        {
                            "format": "csv",
                            "tables": len(tables),
                            "rows_written": total_rows_written,
                            "max_rows_per_table": MAX_EXPORT_ROWS,
                        }
                    ),
                }
            ),
        )

    return _exec