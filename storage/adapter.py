from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from core.errors import ErrorCode, Failure, failure


@dataclass(frozen=True)
class StorageError(Exception):
    failure: Failure

    def __str__(self) -> str:
        return f"{self.failure.code}: {self.failure.message}"


def _validate_storage_key_prefix_whitelist(key: str) -> None:
    if not isinstance(key, str) or not key.strip():
        raise StorageError(failure(ErrorCode.VALIDATION_ERROR, "storage key must be a non-empty string", {"key": key}))
    if key.startswith("/") or key.startswith("\\"):
        raise StorageError(failure(ErrorCode.VALIDATION_ERROR, "storage key must be relative (no absolute paths)", {"key": key}))
    if ".." in key.replace("\\", "/").split("/"):
        raise StorageError(failure(ErrorCode.VALIDATION_ERROR, "storage key must not contain '..' segments", {"key": key}))
    if "\\" in key:
        raise StorageError(failure(ErrorCode.VALIDATION_ERROR, "storage key must not contain backslashes", {"key": key}))
    if not (key.startswith("documents/") or key.startswith("artifacts/")):
        raise StorageError(
            failure(
                ErrorCode.VALIDATION_ERROR,
                "storage key must start with an allowed prefix",
                {"key": key, "allowed_prefixes": ["documents/", "artifacts/"]},
            )
        )


class StorageAdapter(ABC):
    def __init__(self, root_dir: str = "data"):
        if not isinstance(root_dir, str) or not root_dir.strip():
            raise StorageError(failure(ErrorCode.VALIDATION_ERROR, "root_dir must be a non-empty string", {"root_dir": root_dir}))
        self._root_dir = root_dir

    @property
    def root_dir(self) -> str:
        return self._root_dir

    def validate_key(self, key: str) -> None:
        _validate_storage_key_prefix_whitelist(key)

    @abstractmethod
    def put_bytes(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def get_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def list_keys(self, prefix: Optional[str] = None) -> list[str]: ...

    @abstractmethod
    def delete(self, key: str) -> None: ...