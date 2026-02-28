from __future__ import annotations

from typing import Any, Dict, List, Tuple

from core.ordering import sort_dict
from services.document_service import DocumentService
from execution.pdf.provider_registry import resolve_pdf_provider
from execution.pdf.canonicalize import canonicalize_pdf


def make_pdf_merge_execution(documents: DocumentService):
    def _exec(payload: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")

        input_ref = payload.get("input_ref")
        params = payload.get("params")

        if not isinstance(input_ref, dict):
            raise ValueError("input_ref must be a dict")
        if not isinstance(params, dict):
            raise ValueError("params must be a dict")

        doc_ids = input_ref.get("documents")

        if not isinstance(doc_ids, list) or len(doc_ids) == 0:
            raise ValueError("input_ref.documents must be a non-empty list")
        for d in doc_ids:
            if not isinstance(d, str) or not d.strip():
                raise ValueError("input_ref.documents must contain non-empty strings")

        provider_res = resolve_pdf_provider("pdf.merge")
        if provider_res.get("degraded") is True or not provider_res.get("provider"):
            raise ValueError("no_pdf_provider_available_for_merge")

        provider = str(provider_res.get("provider"))
        provider_version = str(provider_res.get("provider_version", ""))

        pdfs: List[bytes] = []
        for doc_id in doc_ids:
            rec = documents.get_document(doc_id)
            pdfs.append(documents.load_bytes(rec))

        merged_bytes: bytes

        if provider == "pymupdf":
            import fitz

            out = fitz.open()
            try:
                for b in pdfs:
                    src = fitz.open(stream=b, filetype="pdf")
                    try:
                        out.insert_pdf(src)
                    finally:
                        src.close()
                merged_bytes = out.write()
            finally:
                out.close()

        elif provider == "pypdf":
            from pypdf import PdfMerger
            from io import BytesIO

            merger = PdfMerger()
            try:
                for b in pdfs:
                    merger.append(BytesIO(b))
                buf = BytesIO()
                merger.write(buf)
                merged_bytes = buf.getvalue()
            finally:
                try:
                    merger.close()
                except Exception:
                    pass

        else:
            raise ValueError("selected_provider_not_supported")

        canon = canonicalize_pdf(merged_bytes)

        manifest = {
            "provider": provider,
            "provider_version": provider_version,
            "canonicalization": canon.manifest,
            "ordered_documents": list(doc_ids),
        }

        return (
            canon.pdf_bytes,
            sort_dict(
                {
                    "artifact_kind": "pdf",
                    "media_type": "application/pdf",
                    "manifest": sort_dict(manifest),
                }
            ),
        )

    return _exec