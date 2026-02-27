from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from storage.adapter import StorageAdapter, StorageError


router = APIRouter(prefix="/artifacts", tags=["artifacts"])


def _as_http_error(status_code: int, message: str, details: Optional[Dict[str, Any]] = None) -> HTTPException:
    payload: Dict[str, Any] = {"message": message}
    if details is not None:
        payload["details"] = details
    return HTTPException(status_code=status_code, detail=payload)


@router.get("/{artifact_id}")
def download_artifact(artifact_id: str) -> Response:
    storage: StorageAdapter = router.state.storage

    if not isinstance(artifact_id, str) or not artifact_id.strip():
        raise _as_http_error(400, "artifact_id must be a non-empty string", {"artifact_id": artifact_id})

    try:
        keys = storage.list_keys(prefix="artifacts/")
    except StorageError as e:
        raise _as_http_error(500, "storage error", e.failure.to_dict())

    matches = [k for k in keys if k.startswith(f"artifacts/{artifact_id}")]
    if not matches:
        raise _as_http_error(404, "artifact not found", {"artifact_id": artifact_id})

    if len(matches) != 1:
        raise _as_http_error(
            409,
            "artifact id is ambiguous in storage",
            {"artifact_id": artifact_id, "matches": matches},
        )

    key = matches[0]

    try:
        data = storage.get_bytes(key)
    except StorageError as e:
        raise _as_http_error(500, "storage error", e.failure.to_dict())

    return Response(content=data, media_type="application/octet-stream")