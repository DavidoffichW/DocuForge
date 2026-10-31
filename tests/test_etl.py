from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient
from openpyxl import load_workbook
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from docuforge.app import create_app
from docuforge.config import Settings
from docuforge.etl import (
    DetectionResult,
    ExportSelection,
    TableCandidate,
    TableCell,
    detect_tables,
    escape_spreadsheet_value,
    selected_table,
)

LOCAL_API_HEADERS = {"X-DocuForge-Client": "local-api"}


def build_table_pdf(*, gridded: bool = True, pages: int = 1) -> bytes:
    output = io.BytesIO()
    pdf = canvas.Canvas(output, pagesize=letter, pageCompression=1)
    rows = [["Name", "Qty", "Value"], ["Alpha", "2", "=1+1"], ["Beta", "3", "11"]]
    for page_index in range(pages):
        x0, y0 = 72, 700
        widths = (150, 90, 110)
        row_height = 28
        if gridded:
            x_positions = [x0]
            for width in widths:
                x_positions.append(x_positions[-1] + width)
            for x in x_positions:
                pdf.line(x, y0, x, y0 - len(rows) * row_height)
            for row_index in range(len(rows) + 1):
                y = y0 - row_index * row_height
                pdf.line(x0, y, x_positions[-1], y)
        pdf.setFont("Helvetica", 10)
        for row_index, row in enumerate(rows):
            x = x0 + 6
            for column_index, value in enumerate(row):
                pdf.drawString(x, y0 - row_index * row_height - 18, value)
                x += widths[column_index]
        pdf.drawString(72, 560, f"Synthetic page {page_index + 1}")
        pdf.showPage()
    pdf.save()
    return output.getvalue()


def test_primary_detection_and_text_fallback_are_page_aware(tmp_path: Path) -> None:
    gridded = tmp_path / "gridded.pdf"
    gridded.write_bytes(build_table_pdf(gridded=True, pages=2))
    primary = detect_tables(gridded, file_id="grid", max_pages=10)
    assert primary.status == "OK"
    assert [candidate.page_index for candidate in primary.candidates] == [0, 1]
    assert {candidate.provider for candidate in primary.candidates} == {"pdfplumber"}
    assert primary.candidates[0].grid()[1] == ["Alpha", "2", "=1+1"]

    ungridded = detect_tables(
        build_table_pdf(gridded=False), file_id="text", max_pages=10
    )
    assert ungridded.status == "OK"
    assert len(ungridded.candidates) == 1
    assert ungridded.candidates[0].provider == "text-grid"
    assert ungridded.candidates[0].grid()[0] == ["Name", "Qty", "Value"]


def test_selection_modes_and_spreadsheet_policy() -> None:
    candidate = TableCandidate(
        id="table-one",
        file_id="source",
        page_index=0,
        table_index=0,
        provider="synthetic",
        bbox=(0.0, 0.0, 10.0, 10.0),
        row_count=2,
        column_count=2,
        score=1.0,
        cells=[
            TableCell(0, 0, "a"),
            TableCell(0, 1, "b"),
            TableCell(1, 0, "=1+1"),
            TableCell(1, 1, "-42"),
        ],
    )
    detection = DetectionResult("OK", 1, [candidate])
    assert selected_table(
        detection, [ExportSelection("table-one", mode="row", row=2)]
    ).rows[0].values == ["=1+1", "-42"]
    assert selected_table(
        detection, [ExportSelection("table-one", mode="column", column=2)]
    ).rows[1].values == ["-42"]
    assert selected_table(
        detection, [ExportSelection("table-one", mode="cell", row=1, column=1)]
    ).rows[0].values == ["a"]
    assert escape_spreadsheet_value("=1+1") == "'=1+1"
    assert escape_spreadsheet_value(" \t@SUM(1,2)") == "' \t@SUM(1,2)"
    assert escape_spreadsheet_value("-42") == "-42"


def _import(client: TestClient, data: bytes) -> dict:
    response = client.post(
        "/api/workspaces/default/documents",
        files={"file": ("tables.pdf", data, "application/pdf")},
        data={"title": "tables.pdf"},
    )
    assert response.status_code == 200
    return response.json()


def test_version_detection_and_exports_are_deterministic_and_safe(tmp_path: Path) -> None:
    app = create_app(Settings(workspace_root=tmp_path / "managed"))
    with TestClient(app, base_url="http://127.0.0.1", headers=LOCAL_API_HEADERS) as client:
        imported = _import(client, build_table_pdf())
        version_id = imported["version"]["id"]
        detection = client.post(f"/api/versions/{version_id}/tables/detect")
        assert detection.status_code == 200
        payload = detection.json()
        assert payload["status"] == "OK"
        assert payload["candidates"][0]["provider"] == "pdfplumber"
        candidate_id = payload["candidates"][0]["id"]
        request = {
            "format": "zip",
            "selections": [{"candidate_id": candidate_id, "mode": "table"}],
        }
        first = client.post(f"/api/versions/{version_id}/tables/export", json=request)
        second = client.post(f"/api/versions/{version_id}/tables/export", json=request)
        assert first.status_code == 200
        assert first.content == second.content
        with zipfile.ZipFile(io.BytesIO(first.content)) as archive:
            assert archive.namelist() == sorted(archive.namelist())
            assert all(item.date_time == (1980, 1, 1, 0, 0, 0) for item in archive.infolist())
            csv_rows = list(
                csv.reader(io.StringIO(archive.read("results.csv").decode("utf-8")))
            )
            assert csv_rows[2][-1] == "'=1+1"
            json_rows = [
                json.loads(line) for line in archive.read("results.jsonl").splitlines()
            ]
            assert json_rows[1]["row"]["col_3"] == "=1+1"
            workbook = load_workbook(io.BytesIO(archive.read("results.xlsx")), data_only=False)
            cell = workbook["results"]["G3"]
            assert cell.value == "'=1+1"
            assert cell.data_type != "f"
            manifest = json.loads(archive.read("manifest.json"))
            assert manifest["document"]["version_id"] == version_id
            assert manifest["schema_version"] == "docuforge-etl/1"


def test_batch_export_is_bounded_ordered_and_isolates_bad_files(tmp_path: Path) -> None:
    settings = Settings(
        workspace_root=tmp_path / "managed",
        etl_max_file_bytes=1_000_000,
        etl_max_total_bytes=2_000_000,
        etl_max_files=3,
        etl_max_pages=10,
    )
    app = create_app(settings)
    with TestClient(app, base_url="http://127.0.0.1", headers=LOCAL_API_HEADERS) as client:
        response = client.post(
            "/api/tables/batch-export",
            files=[
                ("files", ("zeta.pdf", build_table_pdf(), "application/pdf")),
                ("files", ("broken.pdf", b"not a PDF", "application/pdf")),
                ("files", ("alpha.pdf", build_table_pdf(), "application/pdf")),
            ],
            data={"page": "1", "table": "1", "mode": "table", "include_xlsx": "true"},
        )
        assert response.status_code == 200
        assert response.headers["x-docuforge-batch-success"] == "2"
        assert response.headers["x-docuforge-batch-failure"] == "1"
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            report = json.loads(archive.read("batch_report.json"))
            assert [item["filename"] for item in report["files"]] == [
                "alpha.pdf",
                "broken.pdf",
                "zeta.pdf",
            ]
            assert report["success_count"] == 2
            assert report["failure_count"] == 1
            rows = list(csv.reader(io.StringIO(archive.read("results.csv").decode("utf-8"))))
            assert rows[1][4:6] == ["1", "alpha.pdf"]
            assert rows[4][4:6] == ["3", "zeta.pdf"]


def test_batch_file_and_page_limits_fail_without_filesystem_path_api(tmp_path: Path) -> None:
    app = create_app(
        Settings(
            workspace_root=tmp_path / "managed",
            etl_max_file_bytes=64,
            etl_max_total_bytes=2_000_000,
            etl_max_files=2,
            etl_max_pages=1,
        )
    )
    with TestClient(app, base_url="http://127.0.0.1", headers=LOCAL_API_HEADERS) as client:
        response = client.post(
            "/api/tables/batch-export",
            files=[("files", ("large.pdf", build_table_pdf(), "application/pdf"))],
            data={"page": "1", "table": "1", "mode": "table"},
        )
        assert response.status_code == 200
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            report = json.loads(archive.read("batch_report.json"))
            assert report["success_count"] == 0
            assert "file exceeds" in report["files"][0]["message"]

        schema = client.get("/api/openapi.json").json()
        operation = schema["paths"]["/api/tables/batch-export"]["post"]
        serialized = json.dumps(operation).lower()
        assert "directory" not in serialized
        assert "filesystem" not in serialized
        assert '"path"' not in serialized

    page_limited = create_app(
        Settings(
            workspace_root=tmp_path / "page-limited",
            etl_max_file_bytes=1_000_000,
            etl_max_total_bytes=2_000_000,
            etl_max_files=2,
            etl_max_pages=1,
        )
    )
    with TestClient(
        page_limited, base_url="http://127.0.0.1", headers=LOCAL_API_HEADERS
    ) as client:
        response = client.post(
            "/api/tables/batch-export",
            files=[
                ("files", ("a.pdf", build_table_pdf(), "application/pdf")),
                ("files", ("b.pdf", build_table_pdf(), "application/pdf")),
            ],
            data={"page": "1", "table": "1", "mode": "table"},
        )
        assert response.status_code == 200
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            report = json.loads(archive.read("batch_report.json"))
            assert report["success_count"] == 1
            assert report["failure_count"] == 1
            assert "batch page limit" in report["files"][1]["message"]
