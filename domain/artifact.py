from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class ArtifactRecord(BaseModel):
    artifact_id: str = Field(...)
    job_id: str = Field(...)
    artifact_kind: str = Field(...)
    media_type: str = Field(...)
    byte_size: int = Field(...)
    content_sha256: Optional[str] = Field(default=None)
    storage_key: str = Field(...)
    manifest: Optional[Dict[str, Any]] = Field(default=None)

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=False,
    )

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        return {k: data[k] for k in sorted(data.keys())}