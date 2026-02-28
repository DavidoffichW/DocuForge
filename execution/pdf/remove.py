from __future__ import annotations

from typing import Any, Dict, List, Tuple

from core.ordering import sort_dict
from services.document_service import DocumentService
from execution.pdf.provider_registry import resolve_pdf_provider
from execution.pdf.canonicalize import canonicalize_pdf


def make_pdf_remove_execution(documents: DocumentService):
    def _exec(payload: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        input_ref = payload.get("input_ref")
        params = payload.get("params")

        if not isinstance(input_ref, dict) or not isinstance(params, dict):
            raise ValueError("invalid payload")

        document_id = input_ref.get("document_id")
        pages = params.get("pages")

        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError("document_id must be non-empty string")

        if not isinstance(pages, list):
            raise ValueError("pages must be list")

        if not all(isinstance(p, int) and p >= 1 for p in pages):
            raise ValueError("pages must be integers >= 1")

        provider_res = resolve_pdf_provider("pdf.remove")
        if provider_res.get("degraded") or not provider_res.get("provider"):
            raise ValueError("no_pdf_provider_available_for_remove")

        provider = provider_res["provider"]
        provider_version = provider_res.get("provider_version", "")

        rec = documents.get_document(document_id)
        pdf_bytes = documents.load_bytes(rec)

        if provider != "pymupdf":
            raise ValueError("remove requires pymupdf")

        import fitz

        src = fitz.open(stream=pdf_bytes, filetype="pdf")
        out = fitz.open()
        try:
            total_pages = src.page_count
            remove_set = {p - 1 for p in pages}
            for idx in range(total_pages):
                if idx not in remove_set:
                    out.insert_pdf(src, from_page=idx, to_page=idx)
            new_bytes = out.write()
        finally:
            src.close()
            out.close()

        canon = canonicalize_pdf(new_bytes)

        manifest = sort_dict(
            {
                "provider": provider,
                "provider_version": provider_version,
                "page_base": 1,
                "removed_pages": list(pages),
                "canonicalization": canon.manifest,
            }
        )

        return (
            canon.pdf_bytes,
            sort_dict(
                {
                    "artifact_kind": "pdf",
                    "media_type": "application/pdf",
                    "manifest": manifest,
                }
            ),
        )

    return _exec