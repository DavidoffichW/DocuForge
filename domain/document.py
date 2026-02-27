 
from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field


class DocumentRecord(BaseModel):
    document_id: str = Field(...)
    content_sha256: str = Field(...)
    filename: str = Field(...)
    media_type: str = Field(...)
    byte_size: int = Field(...)
    ingest_index: int = Field(...)
    storage_key: str = Field(...)
    id_strategy: str = Field(...)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=False,
    )

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        return {k: data[k] for k in sorted(data.keys())}