from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MAX_UPLOAD_BYTES = 256 * 1024 * 1024
DEFAULT_STREAM_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class Settings:
    workspace_root: Path
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES
    stream_chunk_bytes: int = DEFAULT_STREAM_CHUNK_BYTES

    @classmethod
    def from_environment(cls, workspace_root: Path | None = None) -> Settings:
        selected = workspace_root
        if selected is None:
            selected = Path(os.environ.get("DOCUFORGE_WORKSPACE", ".docuforge-workspace"))
        max_upload = int(os.environ.get("DOCUFORGE_MAX_UPLOAD_BYTES", DEFAULT_MAX_UPLOAD_BYTES))
        if max_upload <= 0:
            raise ValueError("DOCUFORGE_MAX_UPLOAD_BYTES must be positive")
        return cls(workspace_root=selected.resolve(), max_upload_bytes=max_upload)
