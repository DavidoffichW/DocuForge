from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pikepdf
import pytest
from pypdf import PdfReader, PdfWriter

from docuforge.errors import FidelityError, InvalidDocumentError
from docuforge.page_commands import initial_page_state
from docuforge.pdf_engine import PdfEngine, inspect_semantics


def write_pdf(path: Path, data: bytes) -> Path:
    path.write_bytes(data)
    return path


def test_reorder_and_rotation_preserve_interactive_structure(
    tmp_path: Path,
    interactive_pdf_bytes: bytes,
) -> None:
    source = write_pdf(tmp_path / "interactive.pdf", interactive_pdf_bytes)
    before = inspect_semantics(source)
    pages = initial_page_state("version", before["page_count"])
    pages[0]["rotation"] = 90
    result = PdfEngine().materialize(source, pages)
    after = inspect_semantics(result.pdf_bytes)
    assert before["pages"][0]["resources"]["xobjects"][0]["data_sha256"]
    assert after["pages"][0]["rotation"] == 90
    assert after["pages"][0]["external_links"] == before["pages"][0]["external_links"]
    assert after["pages"][0]["annotations"] == before["pages"][0]["annotations"]
    assert after["pages"][0]["resources"] == before["pages"][0]["resources"]
    assert after["fields"] == before["fields"]
    assert after["outlines"] == before["outlines"]
    assert result.byte_reproducible is True


def test_removing_internal_link_target_is_blocked(
    tmp_path: Path,
    interactive_pdf_bytes: bytes,
) -> None:
    source = write_pdf(tmp_path / "interactive.pdf", interactive_pdf_bytes)
    pages = initial_page_state("version", 3)
    with pytest.raises(FidelityError, match="links to a removed page"):
        PdfEngine().materialize(source, [pages[0], pages[2]])


def test_merge_preserves_each_source_structure(
    tmp_path: Path,
    interactive_pdf_bytes: bytes,
    pdf_bytes: bytes,
) -> None:
    first = write_pdf(tmp_path / "first.pdf", interactive_pdf_bytes)
    second = write_pdf(tmp_path / "second.pdf", pdf_bytes)
    result = PdfEngine().merge([first, second])
    merged = inspect_semantics(result.pdf_bytes)
    assert merged["page_count"] == 6
    assert merged["pages"][0]["external_links"] == ["https://example.invalid/docuforge"]
    assert merged["pages"][0]["internal_links"][0]["target"] == 1
    assert merged["fields"] == ["reviewer"]
    assert len(merged["outlines"]) == 6
    assert result.byte_reproducible is True


def test_malformed_and_encrypted_pdfs_are_rejected(tmp_path: Path, pdf_bytes: bytes) -> None:
    malformed = write_pdf(tmp_path / "bad.pdf", b"%PDF-not-valid")
    with pytest.raises(InvalidDocumentError):
        PdfEngine().validate(malformed)
    source = write_pdf(tmp_path / "source.pdf", pdf_bytes)
    encrypted = tmp_path / "encrypted.pdf"
    with pikepdf.Pdf.open(source) as pdf:
        pdf.save(encrypted, encryption=pikepdf.Encryption(owner="owner", user="user"))
    with pytest.raises(Exception, match="encrypted"):
        PdfEngine().validate(encrypted)


def test_semantic_and_byte_hashes_are_separate(tmp_path: Path, pdf_bytes: bytes) -> None:
    source = write_pdf(tmp_path / "source.pdf", pdf_bytes)
    pages = initial_page_state("version", 3)
    first = PdfEngine().materialize(source, pages)
    second = PdfEngine().materialize(source, pages)
    assert first.semantic_sha256 == second.semantic_sha256
    assert first.byte_sha256 == second.byte_sha256


def test_named_destination_survives_reorder(tmp_path: Path, pdf_bytes: bytes) -> None:
    reader = PdfReader(BytesIO(pdf_bytes))
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)
    writer.add_named_destination("chapter-two", 1)
    named = tmp_path / "named.pdf"
    with named.open("wb") as output:
        writer.write(output)
    before = inspect_semantics(named)
    assert before["named_destinations"]["chapter-two"] == 1
    pages = initial_page_state("version", 3)
    pages = [pages[2], pages[1], pages[0]]
    result = PdfEngine().materialize(named, pages)
    after = inspect_semantics(result.pdf_bytes)
    assert after["named_destinations"]["chapter-two"] == 1


def test_preexisting_unresolved_link_remains_unresolved(
    tmp_path: Path,
    interactive_pdf_bytes: bytes,
) -> None:
    source = write_pdf(tmp_path / "source.pdf", interactive_pdf_bytes)
    broken = tmp_path / "broken-link.pdf"
    with pikepdf.Pdf.open(source) as pdf:
        for annotation in pdf.pages[0].obj["/Annots"]:
            if annotation.get("/Dest") is not None:
                annotation["/Dest"] = pikepdf.String("missing-destination")
                break
            action = annotation.get("/A")
            if action is not None and str(action.get("/S")) == "/GoTo":
                action["/D"] = pikepdf.String("missing-destination")
                break
        pdf.save(broken)
    pages = initial_page_state("version", 3)
    result = PdfEngine().materialize(broken, pages)
    internal = inspect_semantics(result.pdf_bytes)["pages"][0]["internal_links"]
    assert {"target": None, "name": "missing-destination"} in internal
