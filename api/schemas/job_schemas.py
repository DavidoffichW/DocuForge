from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from domain.job import JobStatus


class JobRunRequest(BaseModel):
    input_ref: Dict[str, Any] = Field(default_factory=dict)
    operation: str = Field(...)
    params: Dict[str, Any] = Field(default_factory=dict)
    provider_preference: Optional[list[str]] = Field(default=None)
    required_capability: Optional[str] = Field(default=None)

    model_config = ConfigDict(extra="forbid")


class JobResponse(BaseModel):
    degradation: Optional[Dict[str, Any]] = Field(default=None)
    failure: Optional[Dict[str, Any]] = Field(default=None)
    input_ref: Dict[str, Any] = Field(...)
    job_id: str = Field(...)
    operation: str = Field(...)
    output_ref: Optional[Dict[str, Any]] = Field(default=None)
    params: Dict[str, Any] = Field(...)
    status: JobStatus = Field(...)

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_domain(cls, job: Any) -> "JobResponse":
        return cls(**job.to_dict())