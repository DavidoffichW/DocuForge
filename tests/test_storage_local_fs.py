import pytest
from pathlib import Path

from storage.local_fs import LocalFSStorage
from core.errors import StorageCollisionError


def test_write_and_read_roundtrip(tmp_path: Path):
    storage = LocalFSStorage(tmp_path)

    key = "documents/test.pdf"
    data = b"hello"

    storage.write_bytes(key, data)
    read = storage.read_bytes(key)

    assert read == data


def test_idempotent_write_same_content(tmp_path: Path):
    storage = LocalFSStorage(tmp_path)

    key = "documents/test.pdf"
    data = b"hello"

    storage.write_bytes(key, data)
    storage.write_bytes(key, data)

    assert storage.read_bytes(key) == data


def test_collision_raises(tmp_path: Path):
    storage = LocalFSStorage(tmp_path)

    key = "documents/test.pdf"

    storage.write_bytes(key, b"hello")

    with pytest.raises(StorageCollisionError):
        storage.write_bytes(key, b"different")


def test_prefix_enforcement(tmp_path: Path):
    storage = LocalFSStorage(tmp_path)

    with pytest.raises(ValueError):
        storage.write_bytes("../escape.pdf", b"bad")


def test_list_keys_sorted(tmp_path: Path):
    storage = LocalFSStorage(tmp_path)

    storage.write_bytes("documents/b.pdf", b"1")
    storage.write_bytes("documents/a.pdf", b"2")

    keys = storage.list_keys("documents")

    assert keys == sorted(keys)


def test_no_path_traversal(tmp_path: Path):
    storage = LocalFSStorage(tmp_path)

    with pytest.raises(ValueError):
        storage.read_bytes("../outside.pdf")