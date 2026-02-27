from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel


class DocumentRecord(BaseModel):
    document_id: str
    content_sha256: str
    storage_key: str
    byte_size: int = 0
    filename: str = ""
    media_type: str = "application/octet-stream"
    ingest_index: int = 0
    id_strategy: str = ""
    metadata: Optional[Dict[str, Any]] = None

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
            raise TypeError("DocumentRecord is immutable")
        super().__setattr__(name, value)

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        data = super().model_dump(*args, **kwargs)
        return {k: data[k] for k in sorted(data.keys())}

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()