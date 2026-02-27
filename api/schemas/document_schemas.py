from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field


class DocumentResponse(BaseModel):
    document_id: str = Field(...)
    content_sha256: str = Field(...)
    filename: str = Field(...)
    media_type: str = Field(...)
    byte_size: int = Field(...)
    ingest_index: int = Field(...)
    storage_key: str = Field(...)
    id_strategy: str = Field(...)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_domain(cls, d: Any) -> "DocumentResponse":
        return cls(**d.to_dict())