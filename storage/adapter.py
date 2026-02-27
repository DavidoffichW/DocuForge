from __future__ import annotations

from core.errors import Failure, StorageCollisionError, StorageError


class StorageAdapter:
    def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def put_bytes(self, key: str, data: bytes, overwrite: bool = False) -> None:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def read_bytes(self, key: str) -> bytes:
        return self.get_bytes(key)

    def write_bytes(self, key: str, data: bytes, overwrite: bool = False) -> None:
        self.put_bytes(key, data, overwrite=overwrite)