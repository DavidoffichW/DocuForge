from __future__ import annotations

import io
from pathlib import Path

import pytest
from PIL import Image
from reportlab.lib.pagesizes import letter
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

from docuforge.config import Settings
from docuforge.pdf_engine import PdfEngine
from docuforge.repository import Repository
from docuforge.service import DocumentManager
from docuforge.storage import ManagedStorage


def build_pdf(
    *,
    pages: int = 3,
    title: str = "Synthetic fixture",
    interactive: bool = False,
) -> bytes:
    output = io.BytesIO()
    pdf = canvas.Canvas(output, pagesize=letter, pageCompression=1)
    pdf.setTitle(title)
    image_bytes = io.BytesIO()
    Image.new("RGB", (8, 8), (27, 95, 122)).save(image_bytes, format="PNG")
    image_bytes.seek(0)
    fixture_image = ImageReader(image_bytes)
    for index in range(pages):
        page_number = index + 1
        pdf.bookmarkPage(f"page-{page_number}")
        pdf.addOutlineEntry(f"Section {page_number}", f"page-{page_number}", level=0)
        pdf.setFont("Helvetica", 14)
        pdf.drawString(72, 720, f"Fixture page {page_number}")
        pdf.drawImage(fixture_image, 500, 700, width=24, height=24)
        if interactive and index == 0:
            pdf.linkURL("https://example.invalid/docuforge", (72, 680, 260, 700), relative=0)
            if pages > 1:
                pdf.linkRect("next", "page-2", (72, 650, 160, 670), relative=0)
            pdf.textAnnotation("review note", Rect=(300, 650, 320, 670))
            pdf.acroForm.textfield(
                name="reviewer",
                value="DocuForge",
                x=72,
                y=600,
                width=160,
                height=20,
            )
        pdf.showPage()
    pdf.save()
    return output.getvalue()


@pytest.fixture
def pdf_bytes() -> bytes:
    return build_pdf()


@pytest.fixture
def interactive_pdf_bytes() -> bytes:
    return build_pdf(interactive=True)


@pytest.fixture
def large_pdf_bytes() -> bytes:
    return build_pdf(pages=30)


@pytest.fixture
def manager(tmp_path: Path) -> DocumentManager:
    settings = Settings(workspace_root=tmp_path / "managed", max_upload_bytes=2_000_000)
    repository = Repository(settings.workspace_root / "docuforge.sqlite3")
    storage = ManagedStorage(settings.workspace_root)
    return DocumentManager(settings, repository, storage, PdfEngine())
