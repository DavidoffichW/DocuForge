from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple

from core.execution_policy import ExecutionPolicy, ProviderResolution
from core.errors import ErrorCode, failure
from core.ids import HybridDocumentIdStrategy, make_job_id
from domain.job import JobRecord, JobStatus
from services.artifact_service import ArtifactService
from services.document_service import DocumentService


ExecutionFn = Callable[[Dict[str, Any]], Tuple[bytes, Dict[str, Any]]]


class JobService:
    def __init__(
        self,
        policy: ExecutionPolicy,
        documents: Optional[DocumentService] = None,
        artifacts: Optional[ArtifactService] = None,
        execution_map: Optional[Dict[str, ExecutionFn]] = None,
        document_service: Optional[DocumentService] = None,
        artifact_service: Optional[ArtifactService] = None,
        storage: Any = None,
    ):
        self._policy = policy

        self._artifacts = artifacts if artifacts is not None else artifact_service
        if self._artifacts is None:
            raise ValueError("ArtifactService is required")

        if documents is not None:
            self._documents = documents
        elif document_service is not None:
            self._documents = document_service
        else:
            if storage is None:
                raise ValueError("DocumentService or storage must be provided")
            self._documents = DocumentService(
                storage=storage,
                id_strategy=HybridDocumentIdStrategy(),
                policy=policy,
                session_namespace="default_session",
            )

        self._execution_map = dict(execution_map) if execution_map is not None else {}

    def execute(
        self,
        operation: str,
        input_ref: Dict[str, Any],
        params: Dict[str, Any],
        required_capability: Optional[str] = None,
        provider_preference: Optional[list[str]] = None,
    ) -> JobRecord:
        decision = self._policy.require("python_runtime")
        if not decision.allowed:
            raise RuntimeError("capability blocked: python_runtime")
        return self.run(
            operation=operation,
            input_ref=input_ref,
            params=params,
            required_capability=required_capability,
            provider_preference=provider_preference,
        )

    def run(
        self,
        operation: str,
        input_ref: Dict[str, Any],
        params: Dict[str, Any],
        required_capability: Optional[str] = None,
        provider_preference: Optional[list[str]] = None,
    ) -> JobRecord:
        if not isinstance(operation, str) or not operation.strip():
            raise ValueError("operation must be non-empty string")
        if not isinstance(input_ref, dict):
            raise ValueError("input_ref must be dict")
        if not isinstance(params, dict):
            raise ValueError("params must be dict")

        job_id = make_job_id(operation, input_ref, params)

        if required_capability is not None:
            res: ProviderResolution = self._policy.resolve_provider_chain(
                required_capability, preference=provider_preference
            )
            if res.status != "OK":
                return JobRecord(
                    job_id=job_id,
                    operation=operation,
                    status=JobStatus.BLOCKED,
                    input_ref=dict(input_ref),
                    params=dict(params),
                    output_ref=None,
                    failure=res.failure.to_dict() if res.failure else None,
                    degradation=res.degradation,
                )

        if operation not in self._execution_map:
            f = failure(
                ErrorCode.UNKNOWN_OPERATION,
                "unknown operation",
                {"operation": operation},
            )
            return JobRecord(
                job_id=job_id,
                operation=operation,
                status=JobStatus.FAILED,
                input_ref=dict(input_ref),
                params=dict(params),
                output_ref=None,
                failure=f.to_dict(),
                degradation=None,
            )

        fn = self._execution_map[operation]

        running = JobRecord(
            job_id=job_id,
            operation=operation,
            status=JobStatus.RUNNING,
            input_ref=dict(input_ref),
            params=dict(params),
            output_ref=None,
            failure=None,
            degradation=None,
        )

        out_bytes, out_meta = fn(
            {
                "input_ref": dict(input_ref),
                "job": running.to_dict(),
                "params": dict(params),
            }
        )

        if not isinstance(out_bytes, (bytes, bytearray)):
            raise ValueError("execution function must return bytes")
        if not isinstance(out_meta, dict):
            raise ValueError("execution function must return metadata dict")

        kind = str(out_meta.get("kind", out_meta.get("artifact_kind", "bin")))
        media_type = str(out_meta.get("media_type", "application/octet-stream"))
        manifest = out_meta.get("manifest")
        manifest_dict = dict(manifest) if isinstance(manifest, dict) else None

        artifact = self._artifacts.create(
            kind=kind,
            input_ref=dict(input_ref),
            params=dict(params),
            data=bytes(out_bytes),
            media_type=media_type,
            manifest=manifest_dict,
            job_id=job_id,
            compute_content_sha256=True,
        )

        output_ref = {
            "artifact_id": artifact.artifact_id,
            "byte_size": artifact.byte_size,
            "content_sha256": artifact.content_sha256,
            "kind": artifact.kind,
            "media_type": artifact.media_type,
            "storage_key": artifact.storage_key,
        }
        output_ref = {k: output_ref[k] for k in sorted(output_ref.keys())}

        return JobRecord(
            job_id=job_id,
            operation=operation,
            status=JobStatus.COMPLETED,
            input_ref=dict(input_ref),
            params=dict(params),
            output_ref=output_ref,
            failure=None,
            degradation=None,
        )