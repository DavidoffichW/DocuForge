from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

from api.schemas.job_schemas import JobResponse, JobRunRequest
from services.job_service import JobService
from storage.adapter import StorageError


router = APIRouter(prefix="/jobs", tags=["jobs"])


def _as_http_error(status_code: int, message: str, details: Optional[Dict[str, Any]] = None) -> HTTPException:
    payload: Dict[str, Any] = {"message": message}
    if details is not None:
        payload["details"] = details
    return HTTPException(status_code=status_code, detail=payload)


@router.post("/run", response_model=JobResponse)
def run_job(req: JobRunRequest) -> JobResponse:
    svc: JobService = router.state.job_service

    try:
        job = svc.run_job(
            job_type=req.job_type,
            input_ref=dict(req.input_ref),
            params=dict(req.params),
            required_capability=req.required_capability,
            provider_preference=list(req.provider_preference) if req.provider_preference is not None else None,
        )
    except ValueError as e:
        raise _as_http_error(400, str(e))
    except StorageError as e:
        raise _as_http_error(500, "storage error", e.failure.to_dict())
    except Exception as e:
        raise _as_http_error(500, "internal error", {"error": str(e)})

    return JobResponse.from_domain(job)