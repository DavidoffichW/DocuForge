from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pikepdf
import pytest
from PIL import Image
from pypdf import PdfReader, PdfWriter
from pypdf.generic import Fit
from reportlab.lib.pagesizes import letter
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

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
    writer.add_named_destination("chapter-two", 1, Fit.xyz(72, 700, 1.5))
    named = tmp_path / "named.pdf"
    with named.open("wb") as output:
        writer.write(output)
    before = inspect_semantics(named)
    assert before["named_destinations"]["chapter-two"] == {
        "name": "chapter-two",
        "target": 1,
        "type": "/XYZ",
        "parameters": [72.0, 700.0, 1.5],
    }
    pages = initial_page_state("version", 3)
    pages = [pages[2], pages[0], pages[1]]
    result = PdfEngine().materialize(named, pages)
    after = inspect_semantics(result.pdf_bytes)
    assert after["named_destinations"]["chapter-two"] == {
        "name": "chapter-two",
        "target": 2,
        "type": "/XYZ",
        "parameters": [72.0, 700.0, 1.5],
    }


def test_named_destinations_and_goto_actions_keep_view_semantics(
    tmp_path: Path,
    interactive_pdf_bytes: bytes,
) -> None:
    reader = PdfReader(BytesIO(interactive_pdf_bytes))
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)
    writer.add_named_destination("detail-view", 1, Fit.fit_rectangle(10, 20, 400, 700))
    named = tmp_path / "named-action.pdf"
    with named.open("wb") as output:
        writer.write(output)
    with pikepdf.Pdf.open(named, allow_overwriting_input=True) as pdf:
        link = next(
            annotation
            for annotation in pdf.pages[0].obj["/Annots"]
            if str(annotation.get("/Subtype")) == "/Link"
            and annotation.get("/A") is None
        )
        if "/Dest" in link:
            del link["/Dest"]
        link["/A"] = pdf.make_indirect(
            pikepdf.Dictionary(S=pikepdf.Name("/GoTo"), D=pikepdf.String("detail-view"))
        )
        pdf.save(named)

    before = inspect_semantics(named)
    pages = initial_page_state("version", 3)
    reordered = [pages[2], pages[0], pages[1]]
    result = PdfEngine().materialize(named, reordered)
    after = inspect_semantics(result.pdf_bytes)
    expected_destination = {
        "name": "detail-view",
        "target": 2,
        "type": "/FitR",
        "parameters": [10.0, 20.0, 400.0, 700.0],
    }
    assert before["named_destinations"]["detail-view"]["target"] == 1
    assert after["named_destinations"]["detail-view"] == expected_destination
    goto_actions = [
        detail["action"]
        for detail in after["pages"][1]["annotation_details"]
        if detail["action"] and detail["action"]["type"] == "/GoTo"
    ]
    assert goto_actions == [
        {
            "type": "/GoTo",
            "supported": True,
            "destination": expected_destination,
        }
    ]


def test_uri_action_parameters_are_preserved(
    tmp_path: Path,
    interactive_pdf_bytes: bytes,
) -> None:
    source = write_pdf(tmp_path / "uri-action.pdf", interactive_pdf_bytes)
    before = inspect_semantics(source)
    result = PdfEngine().materialize(source, initial_page_state("version", 3))
    after = inspect_semantics(result.pdf_bytes)
    before_uri = [
        detail["action"]
        for detail in before["pages"][0]["annotation_details"]
        if detail["action"] and detail["action"]["type"] == "/URI"
    ]
    after_uri = [
        detail["action"]
        for detail in after["pages"][0]["annotation_details"]
        if detail["action"] and detail["action"]["type"] == "/URI"
    ]
    assert before_uri == after_uri == [
        {
            "type": "/URI",
            "supported": True,
            "uri": "https://example.invalid/docuforge",
            "is_map": False,
        }
    ]


def test_unsupported_annotation_action_blocks_transformation(
    tmp_path: Path,
    interactive_pdf_bytes: bytes,
) -> None:
    source = write_pdf(tmp_path / "unsupported-action.pdf", interactive_pdf_bytes)
    with pikepdf.Pdf.open(source, allow_overwriting_input=True) as pdf:
        link = next(
            annotation
            for annotation in pdf.pages[0].obj["/Annots"]
            if str(annotation.get("/Subtype")) == "/Link"
        )
        link["/A"] = pdf.make_indirect(
            pikepdf.Dictionary(
                S=pikepdf.Name("/JavaScript"),
                JS=pikepdf.String("app.alert('not executed')"),
            )
        )
        if "/Dest" in link:
            del link["/Dest"]
        pdf.save(source)

    with pytest.raises(FidelityError, match="unsupported PDF action /JavaScript"):
        PdfEngine().materialize(source, initial_page_state("version", 3))


def test_merge_preserves_nondefault_named_destination_semantics(
    tmp_path: Path,
    pdf_bytes: bytes,
) -> None:
    sources: list[Path] = []
    for index, fit in enumerate((Fit.xyz(12, 710, 2), Fit.fit_horizontally(650))):
        reader = PdfReader(BytesIO(pdf_bytes))
        writer = PdfWriter()
        writer.clone_document_from_reader(reader)
        writer.add_named_destination(f"section-{index}", 1, fit)
        path = tmp_path / f"source-{index}.pdf"
        with path.open("wb") as output:
            writer.write(output)
        sources.append(path)

    result = PdfEngine().merge(sources)
    merged = inspect_semantics(result.pdf_bytes)["named_destinations"]
    assert merged["section-0"] == {
        "name": "section-0",
        "target": 1,
        "type": "/XYZ",
        "parameters": [12.0, 710.0, 2.0],
    }
    assert merged["section-1"] == {
        "name": "section-1",
        "target": 4,
        "type": "/FitH",
        "parameters": [650.0],
    }


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


def test_resource_hash_budget_does_not_block_page_reorder(tmp_path: Path) -> None:
    output = BytesIO()
    pdf = canvas.Canvas(output, pagesize=letter, pageCompression=1)
    for color in ((180, 30, 60), (30, 120, 180), (80, 150, 40)):
        image_bytes = BytesIO()
        Image.new("RGB", (500, 400), color).save(image_bytes, format="PNG")
        image_bytes.seek(0)
        pdf.drawImage(ImageReader(image_bytes), 50, 250, width=500, height=400)
        pdf.showPage()
    pdf.save()
    source = write_pdf(tmp_path / "image-heavy.pdf", output.getvalue())
    pages = list(reversed(initial_page_state("version", 3)))
    result = PdfEngine().materialize(source, pages)
    assert result.page_count == 3
    assert result.byte_reproducible is True
