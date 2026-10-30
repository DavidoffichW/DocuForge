from __future__ import annotations

import socket
import threading
import time
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
    expect(page.locator("h1")).to_have_text("Open a PDF to begin")
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
    expect(page.locator(".thumbnail-meta").first).to_contain_text("90°")
    page.wait_for_function(
        """() => {
            const canvas = document.querySelector('#viewer .page canvas');
            return canvas && canvas.width > canvas.height;
        }"""
    )

    page.locator("#undo-button").click()
    expect(page.locator(".thumbnail-meta").first).to_contain_text("0°")
    page.wait_for_function(
        """() => {
            const canvas = document.querySelector('#viewer .page canvas');
            return canvas && canvas.height > canvas.width;
        }"""
    )
    page.locator("#redo-button").click()
    expect(page.locator(".thumbnail-meta").first).to_contain_text("90°")

    first = page.locator(".thumbnail").first
    last = page.locator(".thumbnail").last
    first.drag_to(last)
    expect(page.locator(".thumbnail-meta").last).to_contain_text("90°")
    page.locator("#page-number").fill("1")
    page.locator("#page-number").dispatch_event("change")
    expect(page.locator("#viewer .page").first.locator(".textLayer")).to_contain_text(
        "Fixture page 2"
    )

    page.locator("#search-field").fill("Fixture page 1")
    page.locator("#search-button").click()
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
    expect(page.locator(".thumbnail-meta").first).to_contain_text("90°")
    page.locator("#page-number").fill("1")
    page.locator("#page-number").dispatch_event("change")
    expect(page.locator("#viewer .page").first.locator(".textLayer")).to_contain_text(
        "Fixture page 1"
    )
    page.locator("#redo-button").click()
    expect(page.locator(".thumbnail-meta").last).to_contain_text("90°")

    page.locator(".thumbnail input").last.uncheck()
    page.locator(".thumbnail input").nth(1).check()
    page.locator("#delete-pages").click()
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
    expect(page.locator(".thumbnail-meta").first).to_contain_text("0°")

    page.locator("#save-button").click()
    expect(page.locator("#dirty-marker")).to_be_hidden(timeout=15_000)
    expect(page.locator("#version-label")).to_contain_text("Version 2")
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
    expect(page.locator("#version-label")).to_contain_text("Version 2")
    expect(page.locator("#viewer .page").first).to_be_visible(timeout=15_000)
    expect(page.locator("#viewer .page")).to_have_count(2)
    assert page_errors == []


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
