from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel


class ArtifactRecord(BaseModel):
    artifact_id: str
    kind: str
    storage_key: str
    byte_size: int = 0
    media_type: str = "application/octet-stream"
    content_sha256: Optional[str] = None
    job_id: str = ""
    manifest: Optional[Dict[str, Any]] = None

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
            raise TypeError("ArtifactRecord is immutable")
        super().__setattr__(name, value)

    def _dump(self) -> Dict[str, Any]:
        if hasattr(self, "model_dump"):
            return self.model_dump()
        return self.dict()

    def to_dict(self) -> Dict[str, Any]:
        payload = self._dump()
        return {k: payload[k] for k in sorted(payload.keys())}