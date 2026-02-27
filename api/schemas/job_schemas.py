from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class JobRunRequest(BaseModel):
    job_type: str = Field(...)
    input_ref: Dict[str, Any] = Field(...)
    params: Dict[str, Any] = Field(...)
    required_capability: Optional[str] = Field(default=None)
    provider_preference: Optional[List[str]] = Field(default=None)

    model_config = ConfigDict(extra="forbid")


class JobResponse(BaseModel):
    job_id: str = Field(...)
    job_type: str = Field(...)
    input_ref: Dict[str, Any] = Field(...)
    params: Dict[str, Any] = Field(...)
    status: str = Field(...)
    output_ref: Optional[Dict[str, Any]] = Field(default=None)
    failure: Optional[Dict[str, Any]] = Field(default=None)
    degradation: Optional[Dict[str, Any]] = Field(default=None)

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_domain(cls, j: Any) -> "JobResponse":
        return cls(**j.to_dict())