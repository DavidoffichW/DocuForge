from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from docuforge.errors import NotFoundError, RevisionConflictError
from docuforge.page_commands import initial_page_state, validate_page_state


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _load_json(value: str) -> Any:
    return json.loads(value)


class Repository:
    def __init__(self, database_path: Path):
        self.database_path = database_path.resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        try:
            yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS workspaces (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
                    title TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS versions (
                    id TEXT PRIMARY KEY,
                    document_id TEXT NOT NULL REFERENCES documents(id),
                    version_number INTEGER NOT NULL,
                    parent_version_id TEXT REFERENCES versions(id),
                    content_sha256 TEXT NOT NULL,
                    byte_size INTEGER NOT NULL,
                    page_count INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(document_id, id)
                );
                CREATE INDEX IF NOT EXISTS idx_versions_document
                    ON versions(document_id, created_at);
                CREATE TABLE IF NOT EXISTS document_sources (
                    document_id TEXT NOT NULL REFERENCES documents(id),
                    position INTEGER NOT NULL,
                    source_version_id TEXT NOT NULL REFERENCES versions(id),
                    PRIMARY KEY(document_id, position)
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
                    document_id TEXT NOT NULL REFERENCES documents(id),
                    base_version_id TEXT NOT NULL REFERENCES versions(id),
                    pages_json TEXT NOT NULL,
                    history_json TEXT NOT NULL,
                    redo_json TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    dirty INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL REFERENCES sessions(id),
                    revision INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
                    document_id TEXT NOT NULL REFERENCES documents(id),
                    session_id TEXT REFERENCES sessions(id),
                    version_id TEXT REFERENCES versions(id),
                    content_sha256 TEXT NOT NULL,
                    byte_size INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(versions)").fetchall()
            }
            if "version_number" not in columns:
                connection.execute("ALTER TABLE versions ADD COLUMN version_number INTEGER")
                document_ids = connection.execute(
                    "SELECT DISTINCT document_id FROM versions"
                ).fetchall()
                for (document_id,) in document_ids:
                    rows = connection.execute(
                        "SELECT rowid FROM versions WHERE document_id = ? "
                        "ORDER BY created_at, rowid",
                        (document_id,),
                    ).fetchall()
                    for number, (rowid,) in enumerate(rows, start=1):
                        connection.execute(
                            "UPDATE versions SET version_number = ? WHERE rowid = ?",
                            (number, rowid),
                        )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_versions_number "
                "ON versions(document_id, version_number)"
            )
            connection.commit()
        self.ensure_workspace("default", "Default workspace")

    def ensure_workspace(self, workspace_id: str, name: str) -> dict[str, Any]:
        if not workspace_id or not name:
            raise ValueError("workspace id and name are required")
        with self.connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO workspaces(id, name, created_at) VALUES (?, ?, ?)",
                (workspace_id, name, _now()),
            )
            connection.commit()
        return self.get_workspace(workspace_id)

    def create_workspace(self, name: str) -> dict[str, Any]:
        workspace_id = uuid.uuid4().hex
        with self.connect() as connection:
            connection.execute(
                "INSERT INTO workspaces(id, name, created_at) VALUES (?, ?, ?)",
                (workspace_id, name.strip() or "Workspace", _now()),
            )
            connection.commit()
        return self.get_workspace(workspace_id)

    def get_workspace(self, workspace_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT id, name, created_at FROM workspaces WHERE id = ?", (workspace_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("workspace not found")
        return dict(row)

    def list_workspaces(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT id, name, created_at FROM workspaces ORDER BY created_at, id"
            ).fetchall()
        return [dict(row) for row in rows]

    def create_document(
        self,
        *,
        workspace_id: str,
        title: str,
        digest: str,
        byte_size: int,
        page_count: int,
        reason: str = "import",
        source_version_ids: list[str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        document_id = uuid.uuid4().hex
        version_id = uuid.uuid4().hex
        created = _now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO documents(id, workspace_id, title, created_at) VALUES (?, ?, ?, ?)",
                (document_id, workspace_id, title, created),
            )
            connection.execute(
                """
                INSERT INTO versions(
                    id, document_id, version_number, parent_version_id, content_sha256,
                    byte_size, page_count, reason, created_at
                ) VALUES (?, ?, 1, NULL, ?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    document_id,
                    digest,
                    byte_size,
                    page_count,
                    reason,
                    created,
                ),
            )
            for position, source_version_id in enumerate(source_version_ids or []):
                connection.execute(
                    "INSERT INTO document_sources(document_id, position, source_version_id) "
                    "VALUES (?, ?, ?)",
                    (document_id, position, source_version_id),
                )
            connection.commit()
        return self.get_document(document_id), self.get_version(version_id)

    def list_documents(self, workspace_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT d.id, d.workspace_id, d.title, d.created_at,
                       v.id AS latest_version_id, v.version_number AS latest_version_number,
                       v.content_sha256,
                       v.byte_size, v.page_count, v.created_at AS version_created_at
                FROM documents d
                JOIN versions v ON v.id = (
                    SELECT id FROM versions
                    WHERE document_id = d.id
                    ORDER BY created_at DESC, rowid DESC LIMIT 1
                )
                WHERE d.workspace_id = ?
                ORDER BY d.created_at DESC, d.id
                """,
                (workspace_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_document(self, document_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT id, workspace_id, title, created_at FROM documents WHERE id = ?",
                (document_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError("document not found")
        return dict(row)

    def get_version(self, version_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT id, document_id, version_number, parent_version_id, content_sha256,
                       byte_size, page_count, reason, created_at
                FROM versions WHERE id = ?
                """,
                (version_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError("document version not found")
        return dict(row)

    def list_versions(self, document_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, document_id, version_number, parent_version_id, content_sha256,
                       byte_size, page_count, reason, created_at
                FROM versions WHERE document_id = ?
                ORDER BY created_at, rowid
                """,
                (document_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_document_sources(self, document_id: str) -> list[dict[str, Any]]:
        self.get_document(document_id)
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT position, source_version_id
                FROM document_sources
                WHERE document_id = ?
                ORDER BY position
                """,
                (document_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def create_session(self, document_id: str, version_id: str | None = None) -> dict[str, Any]:
        document = self.get_document(document_id)
        if version_id is None:
            versions = self.list_versions(document_id)
            if not versions:
                raise NotFoundError("document has no versions")
            version = versions[-1]
        else:
            version = self.get_version(version_id)
            if version["document_id"] != document_id:
                raise ValueError("version does not belong to document")
        session_id = uuid.uuid4().hex
        pages = initial_page_state(version["id"], version["page_count"])
        created = _now()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO sessions(
                    id, workspace_id, document_id, base_version_id, pages_json,
                    history_json, redo_json, revision, dirty, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, '[]', '[]', 0, 0, ?, ?)
                """,
                (
                    session_id,
                    document["workspace_id"],
                    document_id,
                    version["id"],
                    _json(pages),
                    created,
                    created,
                ),
            )
            connection.commit()
        return self.get_session(session_id)

    def get_session(self, session_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("editing session not found")
        result = dict(row)
        result["pages"] = _load_json(result.pop("pages_json"))
        result["history"] = _load_json(result.pop("history_json"))
        result["redo"] = _load_json(result.pop("redo_json"))
        result["dirty"] = bool(result["dirty"])
        return result

    def update_session(
        self,
        *,
        session_id: str,
        expected_revision: int,
        pages: list[dict[str, Any]],
        history: list[dict[str, Any]],
        redo: list[dict[str, Any]],
        action: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        validate_page_state(pages)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT revision FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("editing session not found")
            if row["revision"] != expected_revision:
                raise RevisionConflictError("editing session changed; refresh and retry")
            revision = expected_revision + 1
            connection.execute(
                """
                UPDATE sessions
                SET pages_json = ?, history_json = ?, redo_json = ?,
                    revision = ?, dirty = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    _json(pages),
                    _json(history),
                    _json(redo),
                    revision,
                    int(bool(history)),
                    _now(),
                    session_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO operations(session_id, revision, action, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (session_id, revision, action, _json(payload), _now()),
            )
            connection.commit()
        return self.get_session(session_id)

    def commit_saved_version(
        self,
        *,
        session_id: str,
        expected_revision: int,
        digest: str,
        byte_size: int,
        page_count: int,
        reason: str = "save",
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        session = self.get_session(session_id)
        if session["revision"] != expected_revision:
            raise RevisionConflictError("editing session changed during save")
        version_id = uuid.uuid4().hex
        created = _now()
        new_pages = initial_page_state(version_id, page_count)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT revision FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if row is None or row["revision"] != expected_revision:
                raise RevisionConflictError("editing session changed during save")
            version_number = connection.execute(
                "SELECT COALESCE(MAX(version_number), 0) + 1 FROM versions WHERE document_id = ?",
                (session["document_id"],),
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO versions(
                    id, document_id, version_number, parent_version_id, content_sha256,
                    byte_size, page_count, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    session["document_id"],
                    version_number,
                    session["base_version_id"],
                    digest,
                    byte_size,
                    page_count,
                    reason,
                    created,
                ),
            )
            connection.execute(
                """
                UPDATE sessions
                SET base_version_id = ?, pages_json = ?, history_json = '[]',
                    redo_json = '[]', revision = ?, dirty = 0, updated_at = ?
                WHERE id = ?
                """,
                (version_id, _json(new_pages), expected_revision + 1, created, session_id),
            )
            connection.execute(
                """
                INSERT INTO operations(session_id, revision, action, payload_json, created_at)
                VALUES (?, ?, 'save', ?, ?)
                """,
                (
                    session_id,
                    expected_revision + 1,
                    _json({"version_id": version_id, "content_sha256": digest}),
                    created,
                ),
            )
            connection.commit()
        return self.get_version(version_id), self.get_session(session_id)

    def create_artifact(
        self,
        *,
        session_id: str,
        digest: str,
        byte_size: int,
        title: str,
        kind: str = "pdf-extract",
    ) -> dict[str, Any]:
        session = self.get_session(session_id)
        artifact_id = uuid.uuid4().hex
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO artifacts(
                    id, workspace_id, document_id, session_id, version_id,
                    content_sha256, byte_size, kind, title, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_id,
                    session["workspace_id"],
                    session["document_id"],
                    session_id,
                    session["base_version_id"],
                    digest,
                    byte_size,
                    kind,
                    title,
                    _now(),
                ),
            )
            connection.commit()
        return self.get_artifact(artifact_id)

    def get_artifact(self, artifact_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("artifact not found")
        return dict(row)
