from __future__ import annotations

import copy
from typing import Any


def initial_page_state(version_id: str, page_count: int) -> list[dict[str, Any]]:
    if page_count <= 0:
        raise ValueError("page_count must be positive")
    return [
        {
            "id": f"{version_id}:page:{index + 1}",
            "source_index": index,
            "rotation": 0,
        }
        for index in range(page_count)
    ]


def validate_page_state(pages: list[dict[str, Any]]) -> None:
    if not isinstance(pages, list) or not pages:
        raise ValueError("a session must contain at least one page")
    ids: set[str] = set()
    source_indices: set[int] = set()
    for page in pages:
        if not isinstance(page, dict):
            raise ValueError("invalid page state")
        page_id = page.get("id")
        source_index = page.get("source_index")
        rotation = page.get("rotation")
        if not isinstance(page_id, str) or not page_id:
            raise ValueError("page id must be non-empty")
        if page_id in ids:
            raise ValueError("page ids must be unique")
        if not isinstance(source_index, int) or source_index < 0:
            raise ValueError("source_index must be non-negative")
        if source_index in source_indices:
            raise ValueError("source pages cannot be duplicated in this milestone")
        if rotation not in {0, 90, 180, 270}:
            raise ValueError("rotation must be 0, 90, 180, or 270")
        ids.add(page_id)
        source_indices.add(source_index)


def apply_page_command(
    pages: list[dict[str, Any]],
    command: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    validate_page_state(pages)
    if not isinstance(command, dict):
        raise ValueError("command must be an object")
    kind = command.get("kind")
    before = copy.deepcopy(pages)
    after = copy.deepcopy(pages)

    if kind == "move":
        page_id = command.get("page_id")
        to_index = command.get("to_index")
        if not isinstance(page_id, str) or not isinstance(to_index, int):
            raise ValueError("move requires page_id and integer to_index")
        if to_index < 0 or to_index >= len(after):
            raise ValueError("move target is out of range")
        current = next((i for i, page in enumerate(after) if page["id"] == page_id), None)
        if current is None:
            raise ValueError("page is not in the session")
        page = after.pop(current)
        after.insert(to_index, page)

    elif kind == "rotate":
        page_ids = command.get("page_ids")
        delta = command.get("delta")
        if not isinstance(page_ids, list) or not page_ids:
            raise ValueError("rotate requires page_ids")
        if delta not in {-270, -180, -90, 90, 180, 270}:
            raise ValueError("rotate delta must be a non-zero multiple of 90")
        requested = set(page_ids)
        found = 0
        for page in after:
            if page["id"] in requested:
                page["rotation"] = (page["rotation"] + delta) % 360
                found += 1
        if found != len(requested):
            raise ValueError("one or more pages are not in the session")

    elif kind == "delete":
        page_ids = command.get("page_ids")
        if not isinstance(page_ids, list) or not page_ids:
            raise ValueError("delete requires page_ids")
        requested = set(page_ids)
        if len(requested) != len(page_ids):
            raise ValueError("page_ids must be unique")
        after = [page for page in after if page["id"] not in requested]
        if len(after) == len(before):
            raise ValueError("no selected page is in the session")
        if not after:
            raise ValueError("a document cannot have every page deleted")

    else:
        raise ValueError("unsupported page command")

    validate_page_state(after)
    if after == before:
        raise ValueError("command does not change the session")
    record = {
        "kind": kind,
        "params": copy.deepcopy(command),
        "before": before,
        "after": copy.deepcopy(after),
    }
    return after, record
