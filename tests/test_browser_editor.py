from __future__ import annotations

import socket
import threading
import time
import zipfile
from io import BytesIO
from pathlib import Path

import pytest
import uvicorn
from playwright.sync_api import expect

from docuforge.app import create_app
from docuforge.config import Settings


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def live_editor(tmp_path: Path):
    port = _free_port()
    app = create_app(Settings(workspace_root=tmp_path / "browser-workspace"))
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.02)
    if not server.started:
        raise RuntimeError("test server did not start")
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


@pytest.mark.browser
def test_browser_open_view_organize_save_and_reopen(
    browser,
    live_editor: str,
    interactive_pdf_bytes: bytes,
) -> None:
    page = browser.new_page(viewport={"width": 1440, "height": 900})
    page_errors: list[Exception] = []
    page.on("pageerror", lambda error: page_errors.append(error))
    page.goto(live_editor)
    expect(page.locator("html")).to_have_attribute("data-theme", "dark")
    expect(page.locator(".brand-mark")).to_have_text("D")
    expect(page.locator("h1")).to_have_text("Open a PDF")
    expect(page.locator("#open-button")).to_have_attribute("aria-label", "Open PDF")
    page.locator("#open-file").set_input_files(
        {
            "name": "browser-fixture.pdf",
            "mimeType": "application/pdf",
            "buffer": interactive_pdf_bytes,
        }
    )
    expect(page.locator("#document-title")).to_have_text("browser-fixture.pdf")
    expect(page.locator("#viewer .page").first).to_be_visible(timeout=15_000)
    expect(page.locator("#viewer .textLayer span").first).to_be_attached(timeout=15_000)
    expect(page.locator(".thumbnail")).to_have_count(3)
    expect(page.locator(".right-panel")).to_have_count(0)
    expect(page.locator("footer")).to_have_count(0)
    for removed_text in (
        "Current document",
        "Page editor status",
        "Transform source pages without rebuilding",
        "Select pages, then rotate, delete, or extract",
        "No selection",
        "Local-first",
        "Same-origin guarded",
    ):
        expect(page.get_by_text(removed_text, exact=True)).to_have_count(0)

    first_page = page.locator("#viewer .page").first
    expect(first_page.locator(".textLayer")).to_contain_text("Fixture page 1")
    page.wait_for_function(
        """() => {
            const canvas = document.querySelector('#viewer .page canvas');
            return canvas && canvas.height > canvas.width;
        }"""
    )
    page.locator(".thumbnail input").first.check()
    page.locator("#rotate-right").click()
    expect(page.locator("#dirty-marker")).to_be_visible()
    expect(page.locator("#save-as-button")).to_have_attribute(
        "aria-label", "Save and download"
    )
    expect(page.locator(".thumbnail").first).to_have_attribute(
        "aria-label", "Page 1, rotated 90 degrees"
    )
    page.wait_for_function(
        """() => {
            const canvas = document.querySelector('#viewer .page canvas');
            return canvas && canvas.width > canvas.height;
        }"""
    )

    page.locator("#undo-button").click()
    expect(page.locator(".thumbnail").first).to_have_attribute("aria-label", "Page 1")
    page.wait_for_function(
        """() => {
            const canvas = document.querySelector('#viewer .page canvas');
            return canvas && canvas.height > canvas.width;
        }"""
    )
    page.locator("#redo-button").click()
    expect(page.locator(".thumbnail").first).to_have_attribute(
        "aria-label", "Page 1, rotated 90 degrees"
    )

    first = page.locator(".thumbnail").first
    last = page.locator(".thumbnail").last
    first.drag_to(last)
    expect(page.locator(".thumbnail").last).to_have_attribute(
        "aria-label", "Page 3, rotated 90 degrees"
    )
    page.locator("#page-number").fill("1")
    page.locator("#page-number").dispatch_event("change")
    expect(page.locator("#viewer .page").first.locator(".textLayer")).to_contain_text(
        "Fixture page 2"
    )

    page.locator("#search-button").click()
    expect(page.locator("#search-field")).to_be_visible()
    page.locator("#search-field").fill("Fixture page 1")
    page.locator("#search-field").press("Enter")
    expect(page.locator("#page-number")).to_have_value("3", timeout=15_000)
    edited_page = page.locator("#viewer .page").nth(2)
    expect(edited_page.locator(".textLayer")).to_contain_text("Fixture page 1")
    internal_link = edited_page.locator(".annotationLayer .linkAnnotation a[href^='#']")
    expect(internal_link).to_be_attached()
    internal_link.click()
    expect(page.locator("#page-number")).to_have_value("1")

    selection = page.evaluate(
        """() => {
            const layer = document.querySelector('#viewer .page .textLayer');
            const selection = window.getSelection();
            selection.removeAllRanges();
            const range = document.createRange();
            range.selectNodeContents(layer);
            selection.addRange(range);
            return {
                ranges: selection.rangeCount,
                collapsed: selection.isCollapsed,
                textContent: layer.textContent,
            };
        }"""
    )
    assert selection["ranges"] == 1
    assert selection["collapsed"] is False
    assert "Fixture page 2" in selection["textContent"]

    page.locator("#undo-button").click()
    expect(page.locator(".thumbnail").first).to_have_attribute(
        "aria-label", "Page 1, rotated 90 degrees"
    )
    page.locator("#page-number").fill("1")
    page.locator("#page-number").dispatch_event("change")
    expect(page.locator("#viewer .page").first.locator(".textLayer")).to_contain_text(
        "Fixture page 1"
    )
    page.locator("#redo-button").click()
    expect(page.locator(".thumbnail").last).to_have_attribute(
        "aria-label", "Page 3, rotated 90 degrees"
    )

    page.locator(".thumbnail input").last.uncheck()
    page.locator(".thumbnail input").nth(1).check()
    page.locator("#delete-pages").click()
    expect(page.locator("#delete-dialog")).to_be_visible()
    expect(page.locator("#delete-dialog-title")).to_have_text("Remove 1 page?")
    page.locator("#delete-dialog button[value='confirm']").click()
    expect(page.locator(".thumbnail")).to_have_count(2)
    expect(page.locator("#viewer .page")).to_have_count(2)
    expect(page.locator("#page-count")).to_have_text("2")
    page.locator("#undo-button").click()
    expect(page.locator("#viewer .page")).to_have_count(3)
    page.locator("#redo-button").click()
    expect(page.locator("#viewer .page")).to_have_count(2)

    page.locator(".thumbnail input").first.check()
    page.evaluate(
        """() => {
            document.querySelector('#rotate-right').click();
            document.querySelector('#rotate-left').click();
        }"""
    )
    expect(page.locator(".thumbnail").first).to_have_attribute("aria-label", "Page 1")

    page.locator("#save-button").click()
    expect(page.locator("#dirty-marker")).to_be_hidden(timeout=15_000)
    expect(page.locator("#save-as-button")).to_have_attribute(
        "aria-label", "Download PDF"
    )
    expect(page.locator("#toast")).to_have_class("toast success")
    expect(page.locator("#viewer .page").first).to_be_visible(timeout=15_000)
    expect(page.locator("#viewer .page")).to_have_count(2)
    expect(page.locator("#viewer .page").first.locator(".textLayer")).to_contain_text(
        "Fixture page 2"
    )
    page.locator("#page-number").fill("2")
    page.locator("#page-number").dispatch_event("change")
    expect(page.locator("#viewer .page").nth(1).locator(".textLayer")).to_contain_text(
        "Fixture page 1"
    )
    page.wait_for_function(
        """() => {
            const canvas = document.querySelectorAll('#viewer .page canvas')[1];
            return canvas && canvas.width > canvas.height;
        }"""
    )

    page.locator(".tab[data-panel='documents-panel']").click()
    expect(page.locator(".document-card")).to_have_count(1)
    page.locator(".document-card").click()
    expect(page.locator("#viewer .page").first).to_be_visible(timeout=15_000)
    expect(page.locator("#viewer .page")).to_have_count(2)
    assert page_errors == []


@pytest.mark.browser
def test_browser_theme_and_import_error_feedback(browser, live_editor: str) -> None:
    page = browser.new_page(viewport={"width": 1280, "height": 760})
    page.goto(live_editor)
    expect(page.locator("#progress")).to_be_hidden()
    expect(page.locator("#toast")).to_be_hidden()
    open_button = page.locator("#open-button")
    open_button.hover()
    page.wait_for_timeout(200)
    assert open_button.evaluate(
        "(element) => getComputedStyle(element, '::after').opacity"
    ) == "1"
    open_button.focus()
    page.wait_for_timeout(200)
    assert open_button.evaluate(
        "(element) => getComputedStyle(element, '::after').opacity"
    ) == "1"
    expect(page.locator("#theme-button")).to_have_attribute(
        "aria-label", "Use light theme"
    )
    page.locator("#theme-button").click()
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    expect(page.locator("#theme-button")).to_have_attribute(
        "aria-label", "Use dark theme"
    )
    page.reload()
    expect(page.locator("html")).to_have_attribute("data-theme", "light")

    page.locator("#open-file").set_input_files(
        {
            "name": "not-a-pdf.pdf",
            "mimeType": "application/pdf",
            "buffer": b"not a PDF",
        }
    )
    expect(page.locator("#toast")).to_be_visible()
    expect(page.locator("#toast")).to_have_class("toast error")
    expect(page.locator("#status-message")).to_contain_text("Import failed")
    expect(page.locator("#empty-state")).to_be_visible()


@pytest.mark.browser
def test_browser_extract_and_large_document_navigation(
    browser,
    live_editor: str,
    large_pdf_bytes: bytes,
) -> None:
    page = browser.new_page(viewport={"width": 1280, "height": 760})
    page.goto(live_editor)
    page.locator("#open-file").set_input_files(
        {
            "name": "large-fixture.pdf",
            "mimeType": "application/pdf",
            "buffer": large_pdf_bytes,
        }
    )
    expect(page.locator(".thumbnail")).to_have_count(30, timeout=15_000)
    page.locator(".thumbnail canvas").first.wait_for(timeout=15_000)
    visible_canvases = page.locator(".thumbnail canvas").count()
    assert 1 <= visible_canvases < 30

    page.locator("#page-number").fill("20")
    page.locator("#page-number").press("Enter")
    expect(page.locator("#page-number")).to_have_value("20", timeout=15_000)

    page.locator(".thumbnail input").first.check()
    with page.expect_download(timeout=15_000) as download_info:
        page.locator("#extract-pages").click()
    download = download_info.value
    assert download.suggested_filename == "extracted-pages.pdf"


@pytest.mark.browser
def test_browser_table_detection_export_and_batch_isolation(
    browser,
    live_editor: str,
    table_pdf_bytes: bytes,
) -> None:
    page = browser.new_page(viewport={"width": 1440, "height": 900})
    page.goto(live_editor)
    page.locator("#open-file").set_input_files(
        {
            "name": "tables.pdf",
            "mimeType": "application/pdf",
            "buffer": table_pdf_bytes,
        }
    )
    expect(page.locator("#viewer .page")).to_have_count(1, timeout=15_000)
    page.locator("#tables-button").click()
    expect(page.locator("#etl-panel")).to_be_visible()
    page.locator("#detect-tables").click()
    expect(page.locator(".table-candidate")).to_have_count(1, timeout=15_000)
    expect(page.locator(".table-candidate")).to_contain_text("3 × 3 · pdfplumber")
    expect(page.locator(".table-preview")).to_contain_text("Alpha")
    page.locator(".table-candidate input").check()
    with page.expect_download(timeout=15_000) as download_info:
        page.locator("[data-table-export='csv']").click()
    csv_download = download_info.value
    assert csv_download.suggested_filename == "docuforge-tables.csv"
    assert "'=1+1" in Path(csv_download.path()).read_text(encoding="utf-8")

    page.locator("#batch-files").set_input_files(
        [
            {
                "name": "valid.pdf",
                "mimeType": "application/pdf",
                "buffer": table_pdf_bytes,
            },
            {
                "name": "broken.pdf",
                "mimeType": "application/pdf",
                "buffer": b"not a PDF",
            },
        ]
    )
    with page.expect_download(timeout=15_000) as batch_info:
        page.locator("#batch-export").click()
    batch_download = batch_info.value
    assert batch_download.suggested_filename == "docuforge-batch-tables.zip"
    with zipfile.ZipFile(BytesIO(Path(batch_download.path()).read_bytes())) as archive:
        report = archive.read("batch_report.json").decode("utf-8")
        assert '"success_count":1' in report
        assert '"failure_count":1' in report
