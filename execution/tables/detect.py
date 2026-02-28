from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.ordering import sort_dict
from services.document_service import DocumentService
from execution.tables.provider_registry import resolve_table_provider
from execution.tables.normalization import normalize_grid


def _json_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _pages_param_to_sorted_list(pages: object) -> Optional[List[int]]:
    if pages is None:
        return None
    if not isinstance(pages, list):
        raise ValueError("pages must be a list of integers >= 1")
    if not all(isinstance(p, int) and p >= 1 for p in pages):
        raise ValueError("pages must be a list of integers >= 1")
    return sorted(set(pages))


def _bbox_pdfplumber_to_pdf_points(page_height: float, bbox: Tuple[float, float, float, float]) -> List[float]:
    x0, top, x1, bottom = bbox
    y0 = float(page_height) - float(bottom)
    y1 = float(page_height) - float(top)
    return [float(x0), float(y0), float(x1), float(y1)]


def make_tables_detect_execution(documents: DocumentService):
    def _exec(payload: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")

        input_ref = payload.get("input_ref")
        params = payload.get("params")

        if not isinstance(input_ref, dict):
            raise ValueError("input_ref must be a dict")
        if not isinstance(params, dict):
            raise ValueError("params must be a dict")

        document_id = params.get("document_id")
        pages_req = _pages_param_to_sorted_list(params.get("pages"))

        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError("document_id must be a non-empty string")

        provider_res = resolve_table_provider()
        if provider_res.get("degraded") is True or not provider_res.get("provider"):
            raise ValueError("no_table_provider_available")

        provider = str(provider_res.get("provider"))
        provider_version = str(provider_res.get("provider_version", ""))

        doc_record = documents.get_document(document_id)
        pdf_bytes = documents.load_bytes(doc_record)

        schema_version = "v1"
        page_base = 1

        tables_out: List[Dict[str, Any]] = []
        parameters: Dict[str, Any] = {}

        if provider == "camelot":
            import tempfile
            from pathlib import Path

            import camelot

            parameters = sort_dict(
                {
                    "engine": "camelot",
                    "flavor": "lattice",
                    "strip_text": "\n",
                    "line_scale": 40,
                }
            )

            with tempfile.TemporaryDirectory() as td:
                p = Path(td) / "in.pdf"
                p.write_bytes(pdf_bytes)

                if pages_req is None:
                    pages_arg = "all"
                    pages_list_for_manifest: Optional[List[int]] = None
                else:
                    pages_arg = ",".join(str(x) for x in pages_req)
                    pages_list_for_manifest = list(pages_req)

                tlist = camelot.read_pdf(
                    str(p),
                    pages=pages_arg,
                    flavor="lattice",
                    strip_text="\n",
                    line_scale=40,
                )

                for i, t in enumerate(tlist):
                    page_num = int(getattr(t, "page", 0))
                    bbox = getattr(t, "_bbox", None)
                    if bbox is None:
                        raise ValueError("camelot_table_bbox_missing")

                    x0, y0, x1, y1 = bbox
                    grid = normalize_grid(t.df.values.tolist())

                    conf = None
                    try:
                        rep = getattr(t, "parsing_report", None)
                        if isinstance(rep, dict):
                            acc = rep.get("accuracy")
                            if isinstance(acc, (int, float)):
                                conf = float(acc)
                    except Exception:
                        conf = None

                    tables_out.append(
                        sort_dict(
                            {
                                "page": page_num,
                                "table_index": int(i + 1),
                                "bbox": [float(x0), float(y0), float(x1), float(y1)],
                                "grid": grid,
                                "confidence": conf,
                            }
                        )
                    )

                parameters = sort_dict(
                    {
                        "engine": "camelot",
                        "flavor": "lattice",
                        "strip_text": "\n",
                        "line_scale": 40,
                        "pages": pages_list_for_manifest,
                    }
                )

        elif provider == "pdfplumber":
            import pdfplumber

            parameters = sort_dict(
                {
                    "engine": "pdfplumber",
                    "table_settings": sort_dict(
                        {
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                            "snap_tolerance": 3,
                            "join_tolerance": 3,
                            "edge_min_length": 3,
                            "min_words_vertical": 3,
                            "min_words_horizontal": 1,
                            "intersection_tolerance": 3,
                            "text_tolerance": 3,
                        }
                    ),
                    "pages": pages_req,
                }
            )

            with pdfplumber.open(pdf_bytes) as pdf:
                page_count = len(pdf.pages)
                if pages_req is None:
                    page_indices = list(range(1, page_count + 1))
                else:
                    page_indices = [p for p in pages_req if 1 <= p <= page_count]
                    if len(page_indices) != len(pages_req):
                        raise ValueError("page out of range")

                table_settings = {
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "snap_tolerance": 3,
                    "join_tolerance": 3,
                    "edge_min_length": 3,
                    "min_words_vertical": 3,
                    "min_words_horizontal": 1,
                    "intersection_tolerance": 3,
                    "text_tolerance": 3,
                }

                t_idx = 1
                for p1 in page_indices:
                    page = pdf.pages[p1 - 1]
                    page_h = float(page.height)
                    found = page.find_tables(table_settings=table_settings)
                    for tb in found:
                        bbox = _bbox_pdfplumber_to_pdf_points(page_h, tb.bbox)
                        raw = tb.extract()
                        grid = normalize_grid(raw)
                        tables_out.append(
                            sort_dict(
                                {
                                    "page": int(p1),
                                    "table_index": int(t_idx),
                                    "bbox": bbox,
                                    "grid": grid,
                                    "confidence": None,
                                }
                            )
                        )
                        t_idx += 1

        else:
            raise ValueError("selected_table_provider_not_supported")

        out_obj = sort_dict(
            {
                "schema_version": schema_version,
                "source_document_id": document_id,
                "page_base": page_base,
                "engine": sort_dict({"provider": provider, "version": provider_version}),
                "parameters": parameters,
                "tables": tables_out,
            }
        )

        return (
            _json_bytes(out_obj),
            sort_dict(
                {
                    "artifact_kind": "bin",
                    "media_type": "application/json",
                    "manifest": sort_dict(
                        {
                            "schema_version": schema_version,
                            "page_base": page_base,
                            "provider": provider,
                            "provider_version": provider_version,
                            "table_count": len(tables_out),
                        }
                    ),
                }
            ),
        )

    return _exec