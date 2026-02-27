from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field


class DocumentResponse(BaseModel):
    byte_size: int = Field(...)
    content_sha256: str = Field(...)
    document_id: str = Field(...)
    filename: str = Field(...)
    id_strategy: str = Field(...)
    ingest_index: int = Field(...)
    media_type: str = Field(...)
    metadata: Dict[str, Any] = Field(...)
    storage_key: str = Field(...)

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_domain(cls, doc: Any) -> "DocumentResponse":
        return cls(**doc.to_dict())