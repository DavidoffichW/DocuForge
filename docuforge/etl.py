"""Table extraction adapted from the MIT-licensed PDF ETL Toolkit project."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import zipfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO, StringIO
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

import pdfplumber
from openpyxl import Workbook
from pypdf import PdfReader

from docuforge.errors import InvalidDocumentError

FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)
FIXED_DOCUMENT_TIME = datetime(1980, 1, 1, tzinfo=UTC)
_DANGEROUS_PREFIX = re.compile(r"^[\x00-\x20]*([=+\-@])")
_NEGATIVE_NUMBER = re.compile(r"^-(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")


@dataclass(frozen=True)
class TableCell:
    row: int
    column: int
    text: str


@dataclass(frozen=True)
class TableCandidate:
    id: str
    file_id: str
    page_index: int
    table_index: int
    provider: str
    bbox: tuple[float, float, float, float]
    row_count: int
    column_count: int
    score: float
    cells: list[TableCell]

    def grid(self) -> list[list[str]]:
        rows = [["" for _ in range(self.column_count)] for _ in range(self.row_count)]
        for cell in self.cells:
            if 0 <= cell.row < self.row_count and 0 <= cell.column < self.column_count:
                rows[cell.row][cell.column] = cell.text
        return rows

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "file_id": self.file_id,
            "page_index": self.page_index,
            "page_number": self.page_index + 1,
            "table_index": self.table_index,
            "provider": self.provider,
            "bbox": list(self.bbox),
            "row_count": self.row_count,
            "column_count": self.column_count,
            "score": self.score,
            "grid": self.grid(),
        }


@dataclass(frozen=True)
class DetectionResult:
    status: str
    page_count: int
    candidates: list[TableCandidate]
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "page_count": self.page_count,
            "candidates": [candidate.as_dict() for candidate in self.candidates],
            "errors": list(self.errors),
        }


@dataclass(frozen=True)
class ExportSelection:
    candidate_id: str
    mode: str = "table"
    row: int | None = None
    column: int | None = None


@dataclass(frozen=True)
class ExportRow:
    file_id: str
    page_index: int
    candidate_id: str
    row_index: int
    values: list[str]


@dataclass(frozen=True)
class ExportTable:
    headers: list[str]
    rows: list[ExportRow]


@dataclass(frozen=True)
class BatchInput:
    filename: str
    data: bytes | None
    error: str | None = None


def _source_for_library(source: Path | bytes) -> str | BytesIO:
    return str(source) if isinstance(source, Path) else BytesIO(source)


def _safe_error(provider: str, page_number: int | None, error: Exception) -> str:
    location = f" page {page_number}" if page_number is not None else ""
    return f"{provider}{location} failed ({type(error).__name__})"


def _candidate_id(file_id: str, page_index: int, provider: str, table_index: int) -> str:
    raw = f"docuforge-table-v1\0{file_id}\0{page_index}\0{provider}\0{table_index}"
    return "table-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _page_count(source: Path | bytes) -> int:
    try:
        reader = PdfReader(_source_for_library(source), strict=False)
        if reader.is_encrypted:
            raise InvalidDocumentError("encrypted PDFs are not supported for table extraction")
        count = len(reader.pages)
    except InvalidDocumentError:
        raise
    except Exception as exc:
        raise InvalidDocumentError("PDF structure could not be read for table extraction") from exc
    if count <= 0:
        raise InvalidDocumentError("PDF has no pages")
    return count


def _candidate_from_rows(
    *,
    file_id: str,
    page_index: int,
    table_index: int,
    provider: str,
    bbox: tuple[float, float, float, float],
    raw: Sequence[Sequence[Any]] | None,
) -> TableCandidate | None:
    if not raw:
        return None
    row_count = len(raw)
    column_count = max((len(row) for row in raw), default=0)
    if row_count <= 0 or column_count <= 0:
        return None
    cells = [
        TableCell(
            row=row_index,
            column=column_index,
            text=(
                ""
                if column_index >= len(row) or row[column_index] is None
                else str(row[column_index]).strip()
            ),
        )
        for row_index, row in enumerate(raw)
        for column_index in range(column_count)
    ]
    width = abs(bbox[2] - bbox[0])
    height = abs(bbox[3] - bbox[1])
    return TableCandidate(
        id=_candidate_id(file_id, page_index, provider, table_index),
        file_id=file_id,
        page_index=page_index,
        table_index=table_index,
        provider=provider,
        bbox=bbox,
        row_count=row_count,
        column_count=column_count,
        score=float((row_count * column_count * 0.6) + (width * height * 0.4)),
        cells=cells,
    )


def _detect_pdfplumber(
    source: Path | bytes,
    file_id: str,
    page_indices: Sequence[int],
) -> tuple[list[TableCandidate], list[str]]:
    candidates: list[TableCandidate] = []
    errors: list[str] = []
    with pdfplumber.open(_source_for_library(source)) as document:
        for page_index in page_indices:
            try:
                tables = document.pages[page_index].find_tables()
            except Exception as exc:
                errors.append(_safe_error("pdfplumber", page_index + 1, exc))
                continue
            for table_index, table in enumerate(tables or []):
                try:
                    candidate = _candidate_from_rows(
                        file_id=file_id,
                        page_index=page_index,
                        table_index=table_index,
                        provider="pdfplumber",
                        bbox=tuple(float(value) for value in table.bbox),
                        raw=table.extract(),
                    )
                except Exception as exc:
                    errors.append(_safe_error("pdfplumber", page_index + 1, exc))
                    continue
                if candidate is not None:
                    candidates.append(candidate)
    return candidates, errors


def _heuristic_candidate(
    file_id: str,
    page_index: int,
    words: Sequence[dict[str, Any]],
) -> TableCandidate | None:
    items = [
        (
            float(word["x0"]),
            float(word["top"]),
            float(word["x1"]),
            float(word["bottom"]),
            str(word["text"]).strip(),
        )
        for word in words
        if word.get("text") is not None and str(word["text"]).strip()
    ]
    if not items:
        return None
    items.sort(key=lambda item: (item[1], item[0]))
    row_keys: list[float] = []
    grouped: dict[int, list[tuple[float, float, float, float, str]]] = {}
    for item in items:
        row_index = next(
            (index for index, row_y in enumerate(row_keys) if abs(row_y - item[1]) <= 4.0),
            None,
        )
        if row_index is None:
            row_index = len(row_keys)
            row_keys.append(item[1])
            grouped[row_index] = []
        grouped[row_index].append(item)
    candidate_rows = sorted(
        (index for index in range(len(row_keys)) if len(grouped[index]) >= 3),
        key=lambda index: row_keys[index],
    )
    if len(candidate_rows) < 3:
        return None
    anchors = sorted(item[0] for item in grouped[candidate_rows[0]])
    if len(anchors) < 2:
        return None
    valid_rows: list[int] = []
    for row_index in candidate_rows:
        assignments = [
            min(range(len(anchors)), key=lambda index: abs(anchors[index] - item[0]))
            for item in grouped[row_index]
        ]
        aligned = all(
            abs(anchors[column] - item[0]) <= 24.0
            for item, column in zip(grouped[row_index], assignments, strict=True)
        )
        if aligned and len(set(assignments)) == len(anchors):
            valid_rows.append(row_index)
    if len(valid_rows) < 3:
        return None
    cells: list[TableCell] = []
    for output_row, row_index in enumerate(valid_rows):
        parts: dict[int, list[tuple[float, str]]] = {}
        for item in sorted(grouped[row_index], key=lambda value: value[0]):
            output_column = min(
                range(len(anchors)), key=lambda index: abs(anchors[index] - item[0])
            )
            parts.setdefault(output_column, []).append((item[0], item[4]))
        for output_column in range(len(anchors)):
            segments = sorted(parts.get(output_column, []), key=lambda value: value[0])
            cells.append(
                TableCell(
                    row=output_row,
                    column=output_column,
                    text=" ".join(text for _, text in segments).strip(),
                )
            )
    bbox = (
        min(item[0] for index in valid_rows for item in grouped[index]),
        min(item[1] for index in valid_rows for item in grouped[index]),
        max(item[2] for index in valid_rows for item in grouped[index]),
        max(item[3] for index in valid_rows for item in grouped[index]),
    )
    width = abs(bbox[2] - bbox[0])
    height = abs(bbox[3] - bbox[1])
    return TableCandidate(
        id=_candidate_id(file_id, page_index, "text-grid", 0),
        file_id=file_id,
        page_index=page_index,
        table_index=0,
        provider="text-grid",
        bbox=bbox,
        row_count=len(valid_rows),
        column_count=len(anchors),
        score=float((len(valid_rows) * len(anchors) * 0.6) + (width * height * 0.4)),
        cells=cells,
    )


def _detect_text_grid(
    source: Path | bytes,
    file_id: str,
    page_indices: Sequence[int],
) -> tuple[list[TableCandidate], list[str]]:
    candidates: list[TableCandidate] = []
    errors: list[str] = []
    with pdfplumber.open(_source_for_library(source)) as document:
        for page_index in page_indices:
            try:
                words = document.pages[page_index].extract_words(
                    x_tolerance=1,
                    y_tolerance=3,
                    keep_blank_chars=False,
                    use_text_flow=False,
                )
                candidate = _heuristic_candidate(file_id, page_index, words)
            except Exception as exc:
                errors.append(_safe_error("text-grid", page_index + 1, exc))
                continue
            if candidate is not None:
                candidates.append(candidate)
    return candidates, errors


def detect_tables(
    source: Path | bytes,
    *,
    file_id: str,
    max_pages: int,
) -> DetectionResult:
    page_count = _page_count(source)
    if page_count > max_pages:
        raise ValueError(f"PDF has {page_count} pages; table-processing limit is {max_pages}")
    remaining = set(range(page_count))
    candidates: list[TableCandidate] = []
    errors: list[str] = []
    for provider in (_detect_pdfplumber, _detect_text_grid):
        if not remaining:
            break
        try:
            found, provider_errors = provider(source, file_id, sorted(remaining))
        except Exception as exc:
            errors.append(_safe_error(provider.__name__.removeprefix("_detect_"), None, exc))
            continue
        candidates.extend(found)
        errors.extend(provider_errors)
        remaining.difference_update(candidate.page_index for candidate in found)
    candidates.sort(
        key=lambda candidate: (
            candidate.page_index,
            candidate.bbox[1],
            candidate.bbox[0],
            -candidate.score,
            candidate.id,
        )
    )
    if candidates:
        return DetectionResult("PARTIAL" if errors else "OK", page_count, candidates, errors)
    if errors:
        return DetectionResult("FAILED", page_count, [], errors)
    return DetectionResult("NO_TABLE", page_count, [], [])


def slice_candidate(
    candidate: TableCandidate,
    *,
    mode: str,
    row: int | None,
    column: int | None,
) -> tuple[list[str], list[list[str]]]:
    grid = candidate.grid()
    if not grid:
        return [], []
    row_count = len(grid)
    column_count = len(grid[0])
    if mode == "table":
        return [f"col_{index + 1}" for index in range(column_count)], grid
    if mode == "row":
        if row is None or not 1 <= row <= row_count:
            raise ValueError("row slice requires an available 1-based row")
        return [f"col_{index + 1}" for index in range(column_count)], [grid[row - 1]]
    if mode == "column":
        if column is None or not 1 <= column <= column_count:
            raise ValueError("column slice requires an available 1-based column")
        return ["col_1"], [[values[column - 1]] for values in grid]
    if mode == "cell":
        if row is None or not 1 <= row <= row_count:
            raise ValueError("cell slice requires an available 1-based row")
        if column is None or not 1 <= column <= column_count:
            raise ValueError("cell slice requires an available 1-based column")
        return ["col_1"], [[grid[row - 1][column - 1]]]
    raise ValueError("slice mode must be table, row, column, or cell")


def escape_spreadsheet_value(value: Any) -> str:
    text = "" if value is None else str(value)
    match = _DANGEROUS_PREFIX.match(text)
    if not match:
        return text
    candidate = text.lstrip("".join(chr(value) for value in range(33)))
    if match.group(1) == "-" and _NEGATIVE_NUMBER.fullmatch(candidate):
        return text
    return "'" + text


def _csv_bytes(table: ExportTable) -> bytes:
    output = StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["file_id", "page_index", "candidate_id", "row_index", *table.headers])
    for row in table.rows:
        writer.writerow(
            [
                row.file_id,
                row.page_index,
                row.candidate_id,
                row.row_index,
                *[escape_spreadsheet_value(value) for value in row.values],
            ]
        )
    return output.getvalue().encode("utf-8")


def _jsonl_bytes(table: ExportTable) -> bytes:
    output = BytesIO()
    for row in table.rows:
        value = {
            "candidate_id": row.candidate_id,
            "file_id": row.file_id,
            "page_index": row.page_index,
            "row": {name: row.values[index] for index, name in enumerate(table.headers)},
            "row_index": row.row_index,
        }
        output.write(
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        )
        output.write(b"\n")
    return output.getvalue()


def _build_zip(members: dict[str, bytes]) -> bytes:
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(members):
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"unsafe ZIP member name: {name}")
            info = zipfile.ZipInfo(name, date_time=FIXED_ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(
                info,
                members[name],
                compress_type=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            )
    return output.getvalue()


def _normalize_xlsx(data: bytes) -> bytes:
    with zipfile.ZipFile(BytesIO(data)) as source:
        members = {name: source.read(name) for name in source.namelist()}
    core = members.get("docProps/core.xml")
    if core is not None:
        core, replacements = re.subn(
            rb"(<dcterms:modified\b[^>]*>)[^<]*(</dcterms:modified>)",
            rb"\g<1>1980-01-01T00:00:00Z\g<2>",
            core,
            count=1,
        )
        if replacements != 1:
            raise ValueError("XLSX modification timestamp could not be normalized")
        members["docProps/core.xml"] = core
    return _build_zip(members)


def _xlsx_bytes(table: ExportTable) -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "results"
    workbook.properties.creator = "DocuForge"
    workbook.properties.lastModifiedBy = "DocuForge"
    workbook.properties.created = FIXED_DOCUMENT_TIME
    workbook.properties.modified = FIXED_DOCUMENT_TIME
    worksheet.append(["file_id", "page_index", "candidate_id", "row_index", *table.headers])
    for row in table.rows:
        worksheet.append(
            [
                row.file_id,
                row.page_index,
                row.candidate_id,
                row.row_index,
                *[escape_spreadsheet_value(value) for value in row.values],
            ]
        )
    output = BytesIO()
    workbook.save(output)
    workbook.close()
    return _normalize_xlsx(output.getvalue())


def _json_bytes(value: Any) -> bytes:
    serialized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (serialized + "\n").encode("utf-8")


def export_bytes(
    table: ExportTable,
    *,
    export_format: str,
    manifest: dict[str, Any],
    include_xlsx: bool = True,
    extra_members: dict[str, bytes] | None = None,
) -> tuple[bytes, str, str]:
    if export_format == "csv":
        return _csv_bytes(table), "text/csv; charset=utf-8", "docuforge-tables.csv"
    if export_format == "jsonl":
        return _jsonl_bytes(table), "application/x-ndjson", "docuforge-tables.jsonl"
    if export_format == "xlsx":
        return (
            _xlsx_bytes(table),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "docuforge-tables.xlsx",
        )
    if export_format != "zip":
        raise ValueError("format must be csv, jsonl, xlsx, or zip")
    members = {
        "manifest.json": _json_bytes(manifest),
        "results.csv": _csv_bytes(table),
        "results.jsonl": _jsonl_bytes(table),
    }
    if include_xlsx:
        members["results.xlsx"] = _xlsx_bytes(table)
    if extra_members:
        members.update(extra_members)
    return _build_zip(members), "application/zip", "docuforge-tables.zip"


def selected_table(
    detection: DetectionResult,
    selections: Sequence[ExportSelection],
) -> ExportTable:
    available = {candidate.id: candidate for candidate in detection.candidates}
    headers: list[str] | None = None
    rows: list[ExportRow] = []
    if not selections:
        raise ValueError("select at least one detected table")
    for selection in selections:
        candidate = available.get(selection.candidate_id)
        if candidate is None:
            raise ValueError("selected table is unavailable in the current document version")
        selected_headers, selected_rows = slice_candidate(
            candidate,
            mode=selection.mode,
            row=selection.row,
            column=selection.column,
        )
        if headers is not None and selected_headers != headers:
            raise ValueError("selected table slices do not share one export schema")
        headers = selected_headers
        for row_index, values in enumerate(selected_rows):
            rows.append(
                ExportRow(
                    file_id=candidate.file_id,
                    page_index=candidate.page_index,
                    candidate_id=candidate.id,
                    row_index=row_index,
                    values=list(values),
                )
            )
    return ExportTable(headers or [], rows)


class ETLService:
    def __init__(self, repository: Any, storage: Any, *, max_pages: int):
        self.repository = repository
        self.storage = storage
        self.max_pages = max_pages

    def detect_version(self, version_id: str) -> tuple[dict[str, Any], DetectionResult]:
        version = self.repository.get_version(version_id)
        source = self.storage.blob_path(version["content_sha256"])
        result = detect_tables(
            source,
            file_id=version["content_sha256"],
            max_pages=self.max_pages,
        )
        return version, result

    def export_version(
        self,
        version_id: str,
        selections: Sequence[ExportSelection],
        export_format: str,
    ) -> tuple[bytes, str, str]:
        version, detection = self.detect_version(version_id)
        if detection.status == "FAILED":
            raise ValueError("table detection failed for this document version")
        table = selected_table(detection, selections)
        manifest = {
            "document": {
                "content_sha256": version["content_sha256"],
                "page_count": version["page_count"],
                "version_id": version["id"],
            },
            "export": {
                "format": export_format,
                "schema": ["file_id", "page_index", "candidate_id", "row_index", *table.headers],
                "selection_count": len(selections),
            },
            "providers": ["pdfplumber", "text-grid"],
            "schema_version": "docuforge-etl/1",
        }
        return export_bytes(table, export_format=export_format, manifest=manifest)

    def batch_export(
        self,
        inputs: Sequence[BatchInput],
        *,
        page: int,
        table_index: int,
        mode: str,
        row: int | None,
        column: int | None,
        include_xlsx: bool,
    ) -> tuple[bytes, dict[str, Any]]:
        if page < 1 or table_index < 1:
            raise ValueError("page and table index must be positive")
        headers: list[str] | None = None
        rows: list[ExportRow] = []
        reports: list[dict[str, Any]] = []
        files: list[dict[str, Any]] = []
        processed_pages = 0
        ordered_inputs = sorted(
            inputs, key=lambda item: (item.filename.casefold(), item.filename)
        )
        for sequence, item in enumerate(ordered_inputs, start=1):
            if item.error or item.data is None:
                reports.append(
                    {
                        "filename": item.filename,
                        "message": item.error or "unavailable",
                        "status": "FAILED",
                    }
                )
                continue
            digest = hashlib.sha256(item.data).hexdigest()
            try:
                page_count = _page_count(item.data)
                if processed_pages + page_count > self.max_pages:
                    raise ValueError("batch page limit would be exceeded")
                processed_pages += page_count
                detection = detect_tables(item.data, file_id=digest, max_pages=self.max_pages)
                page_candidates = [
                    candidate
                    for candidate in detection.candidates
                    if candidate.page_index == page - 1
                ]
                if detection.status == "FAILED":
                    raise ValueError("table detection failed")
                if not page_candidates:
                    raise ValueError(f"no table found on page {page}")
                if table_index > len(page_candidates):
                    raise ValueError(f"table {table_index} is unavailable on page {page}")
                candidate = page_candidates[table_index - 1]
                selected_headers, selected_rows = slice_candidate(
                    candidate, mode=mode, row=row, column=column
                )
                batch_headers = ["seq", "source_filename", *selected_headers]
                if headers is not None and batch_headers != headers:
                    raise ValueError("sliced table schema differs from earlier files")
                headers = batch_headers
                for row_index, values in enumerate(selected_rows):
                    rows.append(
                        ExportRow(
                            file_id=digest,
                            page_index=candidate.page_index,
                            candidate_id=candidate.id,
                            row_index=row_index,
                            values=[str(sequence), item.filename, *values],
                        )
                    )
                reports.append({"filename": item.filename, "message": "exported", "status": "OK"})
                files.append(
                    {
                        "filename": item.filename,
                        "page_count": detection.page_count,
                        "sha256": digest,
                    }
                )
            except Exception as exc:
                reports.append({"filename": item.filename, "message": str(exc), "status": "FAILED"})
        reports.sort(key=lambda value: (value["filename"].casefold(), value["filename"]))
        report = {
            "files": reports,
            "pages_processed": processed_pages,
            "success_count": sum(item["status"] == "OK" for item in reports),
            "failure_count": sum(item["status"] != "OK" for item in reports),
        }
        table = ExportTable(headers or ["seq", "source_filename"], rows)
        manifest = {
            "files": sorted(
                files,
                key=lambda value: (value["filename"].casefold(), value["filename"]),
            ),
            "providers": ["pdfplumber", "text-grid"],
            "schema_version": "docuforge-etl-batch/1",
        }
        data, _, _ = export_bytes(
            table,
            export_format="zip",
            manifest=manifest,
            include_xlsx=include_xlsx,
            extra_members={"batch_report.json": _json_bytes(report)},
        )
        return data, report


def bounded_upload_bytes(stream: BinaryIO, *, max_bytes: int, chunk_bytes: int) -> bytes:
    output = BytesIO()
    total = 0
    while True:
        chunk = stream.read(chunk_bytes)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(f"file exceeds the {max_bytes}-byte batch limit")
        output.write(chunk)
    return output.getvalue()


def batch_total_bytes(inputs: Iterable[BatchInput]) -> int:
    return sum(len(item.data) for item in inputs if item.data is not None)
