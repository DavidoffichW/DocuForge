from __future__ import annotations

from typing import Any, Dict, Tuple

from core.ordering import sort_dict
from services.document_service import DocumentService
from execution.pdf.provider_registry import resolve_pdf_provider


def make_pdf_preview_execution(documents: DocumentService):
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
        page = params.get("page")
        dpi = params.get("dpi", 150)

        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError("document_id must be a non-empty string")
        if not isinstance(page, int) or page < 1:
            raise ValueError("page must be an int >= 1")
        if not isinstance(dpi, int) or dpi != 150:
            raise ValueError("dpi must be 150")

        provider_res = resolve_pdf_provider("pdf.preview")
        if provider_res.get("degraded") is True or not provider_res.get("provider"):
            raise ValueError("no_pdf_provider_available_for_preview")

        provider = str(provider_res.get("provider"))
        provider_version = str(provider_res.get("provider_version", ""))

        doc_record = documents.get_document(document_id)
        pdf_bytes = documents.load_bytes(doc_record)

        if provider == "pymupdf":
            import fitz

            pdf = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                page_index = page - 1
                if page_index < 0 or page_index >= pdf.page_count:
                    raise ValueError("page out of range")

                p = pdf.load_page(page_index)
                scale = float(dpi) / 72.0
                pix = p.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
                out_png = pix.tobytes("png")
            finally:
                pdf.close()

            manifest = sort_dict(
                {
                    "page_base": 1,
                    "page": page,
                    "dpi": dpi,
                    "provider": provider,
                    "provider_version": provider_version,
                }
            )

            return (
                out_png,
                sort_dict(
                    {
                        "artifact_kind": "bin",
                        "media_type": "image/png",
                        "manifest": manifest,
                    }
                ),
            )

        raise ValueError("selected_provider_does_not_support_preview")

    return _exec