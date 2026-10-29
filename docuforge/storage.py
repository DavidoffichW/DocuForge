from __future__ import annotations

import hashlib
import os
import secrets
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from docuforge.errors import DocumentTooLargeError, StorageError


class ManagedStorage:
    """Content-addressed immutable PDF storage with atomic writes."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.blob_root = self.root / "blobs"
        self.incoming_root = self.root / "incoming"
        self.blob_root.mkdir(parents=True, exist_ok=True)
        self.incoming_root.mkdir(parents=True, exist_ok=True)

    def _ensure_inside(self, path: Path) -> Path:
        resolved = path.resolve()
        if resolved != self.root and self.root not in resolved.parents:
            raise StorageError("managed path escapes the workspace")
        return resolved

    def blob_path(self, digest: str) -> Path:
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise ValueError("invalid SHA-256 digest")
        return self._ensure_inside(self.blob_root / digest[:2] / f"{digest}.pdf")

    def stage_stream(
        self,
        stream: BinaryIO,
        *,
        max_bytes: int,
        chunk_bytes: int,
    ) -> tuple[Path, str, int]:
        temp = self._ensure_inside(self.incoming_root / f"{secrets.token_hex(16)}.part")
        digest = hashlib.sha256()
        total = 0
        try:
            with temp.open("xb") as output:
                while True:
                    chunk = stream.read(chunk_bytes)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_bytes:
                        raise DocumentTooLargeError(
                            f"PDF exceeds the {max_bytes}-byte upload limit"
                        )
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            return temp, digest.hexdigest(), total
        except Exception:
            temp.unlink(missing_ok=True)
            raise

    def commit_staged(self, staged: Path, digest: str) -> Path:
        staged = self._ensure_inside(staged)
        destination = self.blob_path(digest)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            staged.unlink(missing_ok=True)
            return destination
        try:
            os.replace(staged, destination)
        except OSError as exc:
            staged.unlink(missing_ok=True)
            raise StorageError("unable to commit managed PDF bytes") from exc
        return destination

    def store_bytes(self, data: bytes) -> tuple[Path, str, int]:
        digest = hashlib.sha256(data).hexdigest()
        destination = self.blob_path(digest)
        if destination.exists():
            return destination, digest, len(data)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = self._ensure_inside(self.incoming_root / f"{secrets.token_hex(16)}.part")
        try:
            with temp.open("xb") as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temp, destination)
        except Exception:
            temp.unlink(missing_ok=True)
            raise
        return destination, digest, len(data)

    def open_blob(self, digest: str) -> BinaryIO:
        return self.blob_path(digest).open("rb")

    def iter_range(
        self,
        digest: str,
        start: int,
        length: int,
        chunk_bytes: int,
    ) -> Iterator[bytes]:
        with self.open_blob(digest) as source:
            source.seek(start)
            remaining = length
            while remaining:
                chunk = source.read(min(chunk_bytes, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk
