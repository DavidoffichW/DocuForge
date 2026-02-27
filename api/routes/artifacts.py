from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from core.errors import ErrorCode
from core.ordering import sort_dict

router = APIRouter()


@router.get("/artifacts/{artifact_id}")
def get_artifact(request: Request, artifact_id: str):
    if not isinstance(artifact_id, str) or not artifact_id.strip():
        raise HTTPException(
            status_code=400,
            detail=sort_dict(
                {
                    "code": ErrorCode.VALIDATION_ERROR.value,
                    "message": "invalid artifact_id",
                    "details": {"artifact_id": artifact_id},
                }
            ),
        )

    artifact_service = request.app.state.artifact_service
    storage = artifact_service.storage

    try:
        record = artifact_service.get(artifact_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=sort_dict(
                {
                    "code": ErrorCode.NOT_FOUND.value,
                    "message": "artifact not found",
                    "details": {"artifact_id": artifact_id},
                }
            ),
        )

    try:
        data = storage.get_bytes(record.storage_key)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=sort_dict(
                {
                    "code": ErrorCode.NOT_FOUND.value,
                    "message": "artifact storage missing",
                    "details": {"artifact_id": artifact_id},
                }
            ),
        )

    media_type = record.media_type if record.media_type else "application/octet-stream"

    return Response(content=data, media_type=media_type)