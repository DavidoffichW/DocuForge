from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class JobStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


class JobRecord(BaseModel):
    job_id: str = Field(...)
    job_type: str = Field(...)
    input_ref: Dict[str, Any] = Field(...)
    params: Dict[str, Any] = Field(...)
    status: JobStatus = Field(...)
    output_ref: Optional[Dict[str, Any]] = Field(default=None)
    failure: Optional[Dict[str, Any]] = Field(default=None)
    degradation: Optional[Dict[str, Any]] = Field(default=None)

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=False,
    )

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        return {k: data[k] for k in sorted(data.keys())}