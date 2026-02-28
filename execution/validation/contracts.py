from __future__ import annotations

from typing import Dict, Set


OPERATION_PARAM_CONTRACT: Dict[str, Set[str]] = {
    "pdf.preview": {"document_id", "page", "dpi"},
    "pdf.merge": set(),
    "pdf.reorder": {"pages"},
    "pdf.remove": {"pages"},
    "pdf.extract": {"pages"},
    "tables.detect": {"document_id", "pages"},
    "tables.export.csv": {"table_detection_bytes", "include_header", "header_row_index"},
    "tables.export.jsonl": {"table_detection_bytes"},
    "tables.export.zip": {"table_detection_bytes", "format"},
}