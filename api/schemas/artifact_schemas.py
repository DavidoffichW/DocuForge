from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class ArtifactResponse(BaseModel):
    artifact_id: str = Field(...)
    job_id: str = Field(...)
    artifact_kind: str = Field(...)
    media_type: str = Field(...)
    byte_size: int = Field(...)
    content_sha256: Optional[str] = Field(default=None)
    storage_key: str = Field(...)
    manifest: Optional[Dict[str, Any]] = Field(default=None)

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_domain(cls, a: Any) -> "ArtifactResponse":
        return cls(**a.to_dict())