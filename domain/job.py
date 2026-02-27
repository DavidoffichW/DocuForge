from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"


class JobRecord(BaseModel):
    job_id: str
    operation: str
    input_ref: Dict[str, Any]
    params: Dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    output_ref: Optional[Dict[str, Any]] = None
    failure: Optional[Dict[str, Any]] = None
    degradation: Optional[Dict[str, Any]] = None

    _locked: bool = False

    class Config:
        extra = "forbid"

    def __init__(self, **data: Any):
        super().__init__(**data)
        object.__setattr__(self, "_locked", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        if getattr(self, "_locked", False):
            raise TypeError("JobRecord is immutable")
        super().__setattr__(name, value)

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        data = super().model_dump(*args, **kwargs)
        if "status" in data and isinstance(data["status"], JobStatus):
            data["status"] = data["status"].value
        return {k: data[k] for k in sorted(data.keys())}

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()