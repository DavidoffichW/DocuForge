from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from core.errors import ErrorCode
from core.ordering import sort_dict
from domain.job import JobStatus

router = APIRouter()


@router.post("/jobs/execute")
def execute_job(request: Request, payload: Dict[str, Any]):
    job_service = request.app.state.job_service

    operation = payload.get("operation")
    input_ref = payload.get("input_ref", {})
    params = payload.get("params", {})

    record = job_service.execute(
        operation=operation,
        input_ref=input_ref,
        params=params,
    )

    if record.status in (JobStatus.FAILED, JobStatus.BLOCKED):
        failure_payload = record.failure or {
            "code": ErrorCode.INTERNAL_ERROR.value,
            "message": "execution failed",
            "details": None,
        }
        raise HTTPException(status_code=400, detail=sort_dict(failure_payload))

    return sort_dict(record.to_dict())