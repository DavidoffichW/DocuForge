from __future__ import annotations

from typing import Any, Dict, Optional

from core.ids import make_artifact_id, sha256_hex
from domain.artifact import ArtifactRecord
from storage.adapter import StorageAdapter


class ArtifactService:
    def __init__(self, storage: StorageAdapter):
        self._storage = storage

    def store_bytes(
        self,
        job_id: str,
        artifact_kind: str,
        media_type: str,
        data: bytes,
        input_ref: Dict[str, Any],
        params: Dict[str, Any],
        manifest: Optional[Dict[str, Any]] = None,
        compute_content_sha256: bool = True,
    ) -> ArtifactRecord:
        if not isinstance(job_id, str) or not job_id.strip():
            raise ValueError("job_id must be a non-empty string")
        if not isinstance(artifact_kind, str) or not artifact_kind.strip():
            raise ValueError("artifact_kind must be a non-empty string")
        if not isinstance(media_type, str) or not media_type.strip():
            raise ValueError("media_type must be a non-empty string")
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("data must be bytes")
        if not isinstance(input_ref, dict):
            raise ValueError("input_ref must be a dict")
        if not isinstance(params, dict):
            raise ValueError("params must be a dict")
        if manifest is not None and not isinstance(manifest, dict):
            raise ValueError("manifest must be a dict if provided")

        artifact_id = make_artifact_id(artifact_kind, input_ref, params)

        ext = self._default_ext_for_kind(artifact_kind)
        storage_key = f"artifacts/{artifact_id}{ext}"

        self._storage.put_bytes(storage_key, bytes(data))

        content_sha = sha256_hex(bytes(data)) if compute_content_sha256 else None

        return ArtifactRecord(
            artifact_id=artifact_id,
            job_id=job_id,
            artifact_kind=artifact_kind,
            media_type=media_type,
            byte_size=len(bytes(data)),
            content_sha256=content_sha,
            storage_key=storage_key,
            manifest=dict(manifest) if manifest is not None else None,
        )

    def load_bytes(self, artifact: ArtifactRecord) -> bytes:
        return self._storage.get_bytes(artifact.storage_key)

    def _default_ext_for_kind(self, artifact_kind: str) -> str:
        k = artifact_kind.strip().lower()
        if k == "pdf":
            return ".pdf"
        if k == "csv":
            return ".csv"
        if k == "zip":
            return ".zip"
        if k == "jsonl":
            return ".jsonl"
        return ".bin"