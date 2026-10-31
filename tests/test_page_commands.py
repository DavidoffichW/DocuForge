from __future__ import annotations

import pytest

from docuforge.page_commands import apply_page_command, initial_page_state


def test_page_commands_are_reversible_records() -> None:
    pages = initial_page_state("version", 4)
    moved, move = apply_page_command(
        pages, {"kind": "move", "page_id": pages[3]["id"], "to_index": 0}
    )
    assert [page["source_index"] for page in moved] == [3, 0, 1, 2]
    assert move["before"] == pages

    rotated, rotation = apply_page_command(
        moved,
        {"kind": "rotate", "page_ids": [moved[0]["id"], moved[1]["id"]], "delta": 90},
    )
    assert [page["rotation"] for page in rotated[:2]] == [90, 90]
    assert rotation["before"] == moved

    reduced, deletion = apply_page_command(
        rotated, {"kind": "delete", "page_ids": [rotated[2]["id"]]}
    )
    assert len(reduced) == 3
    assert deletion["before"] == rotated
    assert deletion["after"] == reduced


@pytest.mark.parametrize(
    "command",
    [
        {"kind": "move", "page_id": "missing", "to_index": 0},
        {"kind": "move", "page_id": "v:page:1", "to_index": 9},
        {"kind": "rotate", "page_ids": ["missing"], "delta": 90},
        {"kind": "delete", "page_ids": ["v:page:1", "v:page:2"]},
        {"kind": "unknown"},
    ],
)
def test_invalid_commands_do_not_mutate_page_state(command: dict[str, object]) -> None:
    pages = initial_page_state("v", 2)
    before = [dict(page) for page in pages]
    with pytest.raises(ValueError):
        apply_page_command(pages, command)
    assert pages == before
