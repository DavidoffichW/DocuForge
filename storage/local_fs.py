from __future__ import annotations

import os
from pathlib import Path
from typing import List, Union

from core.errors import ErrorCode, StorageCollisionError, failure
from core.ordering import sort_strings
from storage.adapter import StorageAdapter


class LocalFSStorage(StorageAdapter):
    def __init__(self, root_dir: Union[str, os.PathLike]):
        root = Path(root_dir)
        root.mkdir(parents=True, exist_ok=True)
        self._root = root.resolve()

    @property
    def root_dir(self) -> Path:
        return self._root

    def _normalize_key(self, key: str) -> str:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("storage key must be a non-empty string")
        k = key.replace("\\", "/").lstrip("/")
        if k == "":
            raise ValueError("storage key must be a non-empty string")
        return k

    def _resolve_key(self, key: str) -> Path:
        k = self._normalize_key(key)
        p = (self._root / k).resolve()
        if p != self._root and self._root not in p.parents:
            raise ValueError("storage key escapes root")
        return p

    def exists(self, key: str) -> bool:
        return self._resolve_key(key).exists()

    def get_bytes(self, key: str) -> bytes:
        p = self._resolve_key(key)
        if not p.exists():
            raise FileNotFoundError(key)
        if not p.is_file():
            raise ValueError("object is not a file")
        return p.read_bytes()

    def put_bytes(self, key: str, data: bytes, overwrite: bool = False) -> None:
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("data must be bytes")
        p = self._resolve_key(key)
        p.parent.mkdir(parents=True, exist_ok=True)

        new_bytes = bytes(data)

        if p.exists() and p.is_file():
            existing = p.read_bytes()
            if existing == new_bytes:
                return
            if not overwrite:
                raise StorageCollisionError(
                    failure(
                        ErrorCode.COLLISION,
                        "object already exists with different content",
                        {"key": self._normalize_key(key)},
                    )
                )

        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_bytes(new_bytes)
        tmp.replace(p)

    def list_keys(self, prefix: str) -> List[str]:
        pref = self._normalize_key(prefix) if prefix is not None else ""
        base = self._resolve_key(pref) if pref else self._root
        if not base.exists():
            return []
        if base.is_file():
            rel = base.relative_to(self._root).as_posix()
            return [rel]

        keys: List[str] = []
        for fp in base.rglob("*"):
            if fp.is_file():
                rel = fp.relative_to(self._root).as_posix()
                keys.append(rel)
        return sort_strings(keys)