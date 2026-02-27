from __future__ import annotations

from typing import Any, Dict, Optional

from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy
from core.ids import make_artifact_id, sha256_hex
from domain.artifact import ArtifactRecord
from storage.adapter import StorageAdapter


class ArtifactService:
    def __init__(self, storage: StorageAdapter, policy: Optional[ExecutionPolicy] = None):
        self._storage = storage
        self._policy = policy if policy is not None else ExecutionPolicy(build_registry())
        self._by_id: Dict[str, ArtifactRecord] = {}

    @property
    def storage(self) -> StorageAdapter:
        return self._storage

    def create(
        self,
        kind: str,
        input_ref: Dict[str, Any],
        params: Dict[str, Any],
        data: bytes,
        media_type: str = "application/octet-stream",
        manifest: Optional[Dict[str, Any]] = None,
        job_id: str = "",
        compute_content_sha256: bool = True,
    ) -> ArtifactRecord:
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError("kind must be a non-empty string")
        if not isinstance(input_ref, dict):
            raise ValueError("input_ref must be a dict")
        if not isinstance(params, dict):
            raise ValueError("params must be a dict")
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("data must be bytes")
        if not isinstance(media_type, str) or not media_type.strip():
            raise ValueError("media_type must be a non-empty string")
        if manifest is not None and not isinstance(manifest, dict):
            raise ValueError("manifest must be a dict if provided")
        if not isinstance(job_id, str):
            raise ValueError("job_id must be a string")

        artifact_id = make_artifact_id(kind=kind, input_ref=input_ref, params=params)
        storage_key = f"artifacts/{artifact_id}.bin"

        self._storage.put_bytes(storage_key, bytes(data), overwrite=True)

        content_sha = sha256_hex(bytes(data)) if compute_content_sha256 else None

        rec = ArtifactRecord(
            artifact_id=artifact_id,
            byte_size=len(bytes(data)),
            content_sha256=content_sha,
            job_id=job_id,
            kind=kind,
            manifest=dict(manifest) if manifest is not None else None,
            media_type=media_type,
            storage_key=storage_key,
        )
        self._by_id[artifact_id] = rec
        return rec

    def get(self, artifact_id: str) -> ArtifactRecord:
        if not isinstance(artifact_id, str) or not artifact_id.strip():
            raise ValueError("artifact_id must be a non-empty string")
        if artifact_id not in self._by_id:
            raise KeyError(artifact_id)
        return self._by_id[artifact_id]