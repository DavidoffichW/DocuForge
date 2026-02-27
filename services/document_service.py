from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.ids import (
    DocumentIdentity,
    HybridDocumentIdStrategy,
    document_identity_from_bytes,
    make_session_namespace,
)
from domain.document import DocumentRecord
from storage.adapter import StorageAdapter


class DocumentService:
    def __init__(self, storage: StorageAdapter, session_namespace: Optional[str] = None):
        self._storage = storage
        self._session_namespace = session_namespace if session_namespace is not None else make_session_namespace()
        self._strategy = HybridDocumentIdStrategy()
        self._ingest_index = 0
        self._docs_by_id: Dict[str, DocumentRecord] = {}

    @property
    def session_namespace(self) -> str:
        return self._session_namespace

    def ingest_bytes(
        self,
        data: bytes,
        filename: str,
        media_type: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DocumentRecord:
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("data must be bytes")
        if not isinstance(filename, str) or not filename.strip():
            raise ValueError("filename must be a non-empty string")
        if not isinstance(media_type, str) or not media_type.strip():
            raise ValueError("media_type must be a non-empty string")

        meta = {} if metadata is None else dict(metadata)

        identity: DocumentIdentity = document_identity_from_bytes(
            data=bytes(data),
            strategy=self._strategy,
            session_namespace=self._session_namespace,
            ingest_index=self._ingest_index,
        )

        storage_key = f"documents/{identity.document_id}.bin"
        self._storage.put_bytes(storage_key, bytes(data))

        record = DocumentRecord(
            document_id=identity.document_id,
            content_sha256=identity.content_sha256,
            filename=filename,
            media_type=media_type,
            byte_size=len(bytes(data)),
            ingest_index=identity.ingest_index,
            storage_key=storage_key,
            id_strategy=identity.strategy,
            metadata=meta,
        )

        self._docs_by_id[record.document_id] = record
        self._ingest_index += 1
        return record

    def load_bytes(self, doc: DocumentRecord) -> bytes:
        return self._storage.get_bytes(doc.storage_key)

    def get_document(self, document_id: str) -> DocumentRecord:
        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError("document_id must be a non-empty string")
        if document_id not in self._docs_by_id:
            raise KeyError(document_id)
        return self._docs_by_id[document_id]

    def list_documents(self) -> List[DocumentRecord]:
        items = list(self._docs_by_id.values())
        items.sort(key=lambda d: (d.ingest_index, d.document_id))
        return items