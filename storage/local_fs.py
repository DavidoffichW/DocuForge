from __future__ import annotations

import os
from typing import Optional

from core.errors import ErrorCode, failure
from core.ids import sha256_hex
from core.ordering import sort_strings

from .adapter import StorageAdapter, StorageError


class LocalFSStorage(StorageAdapter):
    def __init__(self, root_dir: str = "data"):
        super().__init__(root_dir=root_dir)
        self._ensure_root()

    def _ensure_root(self) -> None:
        try:
            os.makedirs(self._root_dir, exist_ok=True)
            os.makedirs(os.path.join(self._root_dir, "documents"), exist_ok=True)
            os.makedirs(os.path.join(self._root_dir, "artifacts"), exist_ok=True)
        except Exception as e:
            raise StorageError(
                failure(
                    ErrorCode.STORAGE_ERROR,
                    "failed to initialize storage root directories",
                    {"root_dir": self._root_dir, "error": str(e)},
                )
            )

    def _path_for_key(self, key: str) -> str:
        self.validate_key(key)
        parts = key.split("/")
        return os.path.join(self._root_dir, *parts)

    def put_bytes(self, key: str, data: bytes) -> None:
        self.validate_key(key)
        if not isinstance(data, (bytes, bytearray)):
            raise StorageError(failure(ErrorCode.VALIDATION_ERROR, "data must be bytes", {"key": key}))

        path = self._path_for_key(key)
        parent = os.path.dirname(path)

        try:
            os.makedirs(parent, exist_ok=True)
        except Exception as e:
            raise StorageError(
                failure(
                    ErrorCode.STORAGE_ERROR,
                    "failed to create parent directories",
                    {"key": key, "parent": parent, "error": str(e)},
                )
            )

        if os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    existing = f.read()
            except Exception as e:
                raise StorageError(
                    failure(
                        ErrorCode.STORAGE_ERROR,
                        "failed to read existing object for collision check",
                        {"key": key, "error": str(e)},
                    )
                )

            if sha256_hex(existing) == sha256_hex(bytes(data)):
                return

            raise StorageError(
                failure(
                    ErrorCode.STORAGE_ERROR,
                    "storage key collision with different content",
                    {
                        "key": key,
                        "existing_sha256": sha256_hex(existing),
                        "new_sha256": sha256_hex(bytes(data)),
                    },
                )
            )

        try:
            with open(path, "wb") as f:
                f.write(bytes(data))
        except Exception as e:
            raise StorageError(
                failure(
                    ErrorCode.STORAGE_ERROR,
                    "failed to write object",
                    {"key": key, "path": path, "error": str(e)},
                )
            )

    def get_bytes(self, key: str) -> bytes:
        path = self._path_for_key(key)
        if not os.path.exists(path):
            raise StorageError(
                failure(
                    ErrorCode.STORAGE_ERROR,
                    "object not found",
                    {"key": key},
                )
            )
        try:
            with open(path, "rb") as f:
                return f.read()
        except Exception as e:
            raise StorageError(
                failure(
                    ErrorCode.STORAGE_ERROR,
                    "failed to read object",
                    {"key": key, "path": path, "error": str(e)},
                )
            )

    def exists(self, key: str) -> bool:
        path = self._path_for_key(key)
        return os.path.exists(path)

    def list_keys(self, prefix: Optional[str] = None) -> list[str]:
        if prefix is not None:
            if not isinstance(prefix, str) or not prefix.strip():
                raise StorageError(failure(ErrorCode.VALIDATION_ERROR, "prefix must be a non-empty string if provided", {"prefix": prefix}))
            if not (prefix.startswith("documents/") or prefix.startswith("artifacts/")):
                raise StorageError(
                    failure(
                        ErrorCode.VALIDATION_ERROR,
                        "prefix must start with an allowed prefix",
                        {"prefix": prefix, "allowed_prefixes": ["documents/", "artifacts/"]},
                    )
                )
            if "\\" in prefix:
                raise StorageError(failure(ErrorCode.VALIDATION_ERROR, "prefix must not contain backslashes", {"prefix": prefix}))
            if ".." in prefix.replace("\\", "/").split("/"):
                raise StorageError(failure(ErrorCode.VALIDATION_ERROR, "prefix must not contain '..' segments", {"prefix": prefix}))

        keys: list[str] = []
        base_dir = self._root_dir

        start_dir = base_dir
        if prefix is not None:
            start_dir = os.path.join(base_dir, *prefix.split("/"))

        if not os.path.exists(start_dir):
            return []

        for root, _, files in os.walk(start_dir):
            rel_root = os.path.relpath(root, base_dir)
            rel_root = "" if rel_root == "." else rel_root.replace("\\", "/")
            for fname in files:
                rel = f"{rel_root}/{fname}" if rel_root else fname
                rel = rel.replace("\\", "/")
                if prefix is None:
                    if rel.startswith("documents/") or rel.startswith("artifacts/"):
                        keys.append(rel)
                else:
                    if rel.startswith(prefix):
                        keys.append(rel)

        return sort_strings(keys)

    def delete(self, key: str) -> None:
        path = self._path_for_key(key)
        if not os.path.exists(path):
            return
        try:
            os.remove(path)
        except Exception as e:
            raise StorageError(
                failure(
                    ErrorCode.STORAGE_ERROR,
                    "failed to delete object",
                    {"key": key, "path": path, "error": str(e)},
                )
            )