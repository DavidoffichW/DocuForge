from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, HTTPException, Request, UploadFile

from api.schemas.document_schemas import DocumentResponse
from core.errors import ErrorCode, failure
from services.document_service import DocumentService


router = APIRouter(tags=["documents"])


def _svc(request: Request) -> DocumentService:
    return request.app.state.document_service


def _raise(code: ErrorCode, message: str, details: Optional[Dict[str, Any]], status: int):
    f = failure(code, message, details)
    raise HTTPException(status_code=status, detail=f.to_dict())


@router.post("/documents/upload", response_model=DocumentResponse)
@router.post("/workspaces/{workspace_id}/documents/upload", response_model=DocumentResponse)
async def upload_document(request: Request, file: UploadFile = File(...), workspace_id: str = "default"):
    raw = await file.read()
    filename = file.filename or ""
    media_type = file.content_type or "application/octet-stream"

    try:
        doc = _svc(request).ingest(
            data=raw,
            ingest_index=0,
            filename=filename,
            media_type=media_type,
            metadata={"workspace_id": workspace_id},
        )
    except ValueError as e:
        _raise(ErrorCode.VALIDATION_ERROR, str(e), {"workspace_id": workspace_id}, 400)

    return DocumentResponse.from_domain(doc)


@router.get("/documents", response_model=List[DocumentResponse])
@router.get("/workspaces/{workspace_id}/documents", response_model=List[DocumentResponse])
def list_documents(request: Request, workspace_id: str = "default"):
    docs = _svc(request).list_documents()
    filtered = [d for d in docs if d.metadata.get("workspace_id") == workspace_id]
    return [DocumentResponse.from_domain(d) for d in filtered]


@router.get("/documents/{document_id}", response_model=DocumentResponse)
@router.get("/workspaces/{workspace_id}/documents/{document_id}", response_model=DocumentResponse)
def get_document(request: Request, document_id: str, workspace_id: str = "default"):
    try:
        doc = _svc(request).get_document(document_id)
    except ValueError as e:
        _raise(ErrorCode.VALIDATION_ERROR, str(e), {"workspace_id": workspace_id}, 400)
    except KeyError:
        _raise(ErrorCode.NOT_FOUND, "document not found", {"document_id": document_id}, 404)

    if doc.metadata.get("workspace_id") != workspace_id:
        _raise(ErrorCode.NOT_FOUND, "document not found", {"document_id": document_id}, 404)

    return DocumentResponse.from_domain(doc)