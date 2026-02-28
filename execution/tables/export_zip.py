from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime
from typing import Any, Dict, List, Tuple

from core.ordering import sort_dict

from execution.tables.export_csv import MAX_EXPORT_ROWS as MAX_ROWS_CSV
from execution.tables.export_jsonl import MAX_EXPORT_ROWS as MAX_ROWS_JSONL


ZIP_FIXED_DT = (1980, 1, 1, 0, 0, 0)
ZIP_COMPRESSION = zipfile.ZIP_DEFLATED
ZIP_COMPRESSLEVEL = 6


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


def _table_filename(page: int, idx: int, ext: str) -> str:
    return f"table_p{str(page).zfill(3)}_t{str(idx).zfill(2)}.{ext}"


def _grid_to_csv_bytes(grid: List[List[Any]]) -> bytes:
    import csv

    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if len(grid) > MAX_ROWS_CSV:
        raise ValueError("export row limit exceeded")
    for r in grid:
        if not isinstance(r, list):
            raise ValueError("table row must be list")
        w.writerow(["" if c is None else str(c) for c in r])
    return buf.getvalue().encode("utf-8")


def _grid_to_jsonl_bytes(grid: List[List[Any]]) -> bytes:
    if len(grid) > MAX_ROWS_JSONL:
        raise ValueError("export row limit exceeded")
    lines: List[str] = []
    for r in grid:
        if not isinstance(r, list):
            raise ValueError("table row must be list")
        obj: Dict[str, str] = {}
        for i, c in enumerate(r):
            obj[f"c{str(i + 1).zfill(3)}"] = "" if c is None else str(c)
        lines.append(json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    return ("\n".join(lines)).encode("utf-8")


def make_tables_export_zip_execution():
    def _exec(payload: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        obj = _load_detection_json(payload)
        params = payload.get("params")
        if not isinstance(params, dict):
            raise ValueError("params must be dict")

        ext = params.get("format", "csv")
        if ext not in ("csv", "jsonl"):
            raise ValueError("format must be csv or jsonl")

        tables = _iter_tables(obj)
        if len(tables) == 0:
            raise ValueError("no tables to export")

        entries: List[Tuple[str, bytes]] = []
        for t in tables:
            page = t.get("page")
            idx = t.get("table_index")
            grid = t.get("grid")

            if not isinstance(page, int) or page < 1:
                raise ValueError("table page must be int >= 1")
            if not isinstance(idx, int) or idx < 1:
                raise ValueError("table_index must be int >= 1")
            if not isinstance(grid, list):
                raise ValueError("grid must be list")

            filename = _table_filename(page, idx, ext)

            if ext == "csv":
                data = _grid_to_csv_bytes(grid)
            else:
                data = _grid_to_jsonl_bytes(grid)

            entries.append((filename, data))

        entries.sort(key=lambda x: x[0])

        mem = io.BytesIO()
        with zipfile.ZipFile(
            mem,
            mode="w",
            compression=ZIP_COMPRESSION,
            compresslevel=ZIP_COMPRESSLEVEL,
        ) as zf:
            for name, data in entries:
                zi = zipfile.ZipInfo(filename=name, date_time=ZIP_FIXED_DT)
                zi.compress_type = ZIP_COMPRESSION
                zf.writestr(zi, data)

        out_bytes = mem.getvalue()

        return (
            out_bytes,
            sort_dict(
                {
                    "artifact_kind": "bin",
                    "media_type": "application/zip",
                    "manifest": sort_dict(
                        {
                            "format": "zip",
                            "entry_format": ext,
                            "entries": len(entries),
                            "max_rows_per_table": MAX_ROWS_CSV if ext == "csv" else MAX_ROWS_JSONL,
                            "zip_timestamp": "1980-01-01T00:00:00",
                            "zip_compresslevel": ZIP_COMPRESSLEVEL,
                        }
                    ),
                }
            ),
        )

    return _exec