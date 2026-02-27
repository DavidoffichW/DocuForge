from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from api.schemas.document_schemas import DocumentResponse
from services.document_service import DocumentService


router = APIRouter(prefix="/documents", tags=["documents"])


def _as_http_error(status_code: int, message: str, details: Optional[Dict[str, Any]] = None) -> HTTPException:
    payload: Dict[str, Any] = {"message": message}
    if details is not None:
        payload["details"] = details
    return HTTPException(status_code=status_code, detail=payload)


@router.post("/upload", response_model=DocumentResponse)
async def upload_document(
    file: UploadFile = File(...),
    filename: Optional[str] = Form(default=None),
    media_type: Optional[str] = Form(default=None),
) -> DocumentResponse:
    svc: DocumentService = router.state.document_service

    raw = await file.read()

    resolved_filename = filename if filename is not None else (file.filename or "upload.bin")
    resolved_media_type = media_type if media_type is not None else (file.content_type or "application/octet-stream")

    try:
        doc = svc.ingest_bytes(
            data=raw,
            filename=resolved_filename,
            media_type=resolved_media_type,
            metadata={},
        )
    except ValueError as e:
        raise _as_http_error(400, str(e))
    except Exception as e:
        raise _as_http_error(500, "internal error", {"error": str(e)})

    return DocumentResponse.from_domain(doc)


@router.get("/{document_id}", response_model=DocumentResponse)
def get_document(document_id: str) -> DocumentResponse:
    svc: DocumentService = router.state.document_service
    try:
        doc = svc.get_document(document_id)
    except ValueError as e:
        raise _as_http_error(400, str(e))
    except KeyError:
        raise _as_http_error(404, "document not found", {"document_id": document_id})
    except Exception as e:
        raise _as_http_error(500, "internal error", {"error": str(e)})

    return DocumentResponse.from_domain(doc)


@router.get("", response_model=List[DocumentResponse])
def list_documents() -> List[DocumentResponse]:
    svc: DocumentService = router.state.document_service
    docs = svc.list_documents()
    return [DocumentResponse.from_domain(d) for d in docs]