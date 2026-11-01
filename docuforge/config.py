from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MAX_UPLOAD_BYTES = 256 * 1024 * 1024
DEFAULT_STREAM_CHUNK_BYTES = 1024 * 1024
DEFAULT_ETL_MAX_FILE_BYTES = 50 * 1024 * 1024
DEFAULT_ETL_MAX_TOTAL_BYTES = 250 * 1024 * 1024
DEFAULT_ETL_MAX_FILES = 100
DEFAULT_ETL_MAX_PAGES = 2_000
DEFAULT_MERGE_MAX_FILES = 50
DEFAULT_MERGE_MAX_TOTAL_BYTES = 512 * 1024 * 1024
DEFAULT_MERGE_MAX_PAGES = 5_000


@dataclass(frozen=True)
class Settings:
    workspace_root: Path
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES
    stream_chunk_bytes: int = DEFAULT_STREAM_CHUNK_BYTES
    etl_max_file_bytes: int = DEFAULT_ETL_MAX_FILE_BYTES
    etl_max_total_bytes: int = DEFAULT_ETL_MAX_TOTAL_BYTES
    etl_max_files: int = DEFAULT_ETL_MAX_FILES
    etl_max_pages: int = DEFAULT_ETL_MAX_PAGES
    merge_max_files: int = DEFAULT_MERGE_MAX_FILES
    merge_max_total_bytes: int = DEFAULT_MERGE_MAX_TOTAL_BYTES
    merge_max_pages: int = DEFAULT_MERGE_MAX_PAGES

    @classmethod
    def from_environment(cls, workspace_root: Path | None = None) -> Settings:
        selected = workspace_root
        if selected is None:
            selected = Path(os.environ.get("DOCUFORGE_WORKSPACE", ".docuforge-workspace"))
        max_upload = int(os.environ.get("DOCUFORGE_MAX_UPLOAD_BYTES", DEFAULT_MAX_UPLOAD_BYTES))
        if max_upload <= 0:
            raise ValueError("DOCUFORGE_MAX_UPLOAD_BYTES must be positive")
        values = {
            "etl_max_file_bytes": int(
                os.environ.get("DOCUFORGE_ETL_MAX_FILE_BYTES", DEFAULT_ETL_MAX_FILE_BYTES)
            ),
            "etl_max_total_bytes": int(
                os.environ.get("DOCUFORGE_ETL_MAX_TOTAL_BYTES", DEFAULT_ETL_MAX_TOTAL_BYTES)
            ),
            "etl_max_files": int(
                os.environ.get("DOCUFORGE_ETL_MAX_FILES", DEFAULT_ETL_MAX_FILES)
            ),
            "etl_max_pages": int(
                os.environ.get("DOCUFORGE_ETL_MAX_PAGES", DEFAULT_ETL_MAX_PAGES)
            ),
            "merge_max_files": int(
                os.environ.get("DOCUFORGE_MERGE_MAX_FILES", DEFAULT_MERGE_MAX_FILES)
            ),
            "merge_max_total_bytes": int(
                os.environ.get(
                    "DOCUFORGE_MERGE_MAX_TOTAL_BYTES", DEFAULT_MERGE_MAX_TOTAL_BYTES
                )
            ),
            "merge_max_pages": int(
                os.environ.get("DOCUFORGE_MERGE_MAX_PAGES", DEFAULT_MERGE_MAX_PAGES)
            ),
        }
        if any(value <= 0 for value in values.values()):
            raise ValueError("DocuForge ETL and merge limits must be positive")
        return cls(workspace_root=selected.resolve(), max_upload_bytes=max_upload, **values)
