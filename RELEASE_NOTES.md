# DocuForge v0.2.0

DocuForge now brings page editing and structured PDF extraction into one local
workspace.

## What changed

- Merge several imported or previously saved PDFs in a chosen order. The result
  is a normal managed document with its own immutable initial version.
- Detect tables page by page with pdfplumber and a deterministic text fallback.
- Select a whole table, row, column, or cell and export CSV, JSONL, XLSX, or a
  reproducible ZIP.
- Run the same fixed table selection across several local PDFs with per-file
  failure isolation and explicit byte, file, and page limits.
- Keep formula-like PDF text literal in spreadsheet-oriented exports.
- Preserve the existing PDF.js viewer, page organizer, undo/redo, extraction,
  version history, local browser boundary, and conservative PDF fidelity checks.

The structured-extraction workflow consolidates the useful behavior of the
historical PDF ETL Toolkit into DocuForge. Browser file and folder selection
replace arbitrary server-directory input.

## Install

```bash
python -m venv .venv
python -m pip install dist/docuforge-0.2.0-py3-none-any.whl
python -m docuforge
```

Open <http://127.0.0.1:8000>.

## Limits

OCR, annotation authoring, and editing existing page text or images are not
implemented. Table extraction requires a usable text layer and remains
layout-dependent. PDFs containing unsupported interactive structures are
rejected when a transformation cannot preserve them safely.

DocuForge is a trusted, single-user, loopback application. Do not expose it as
a shared network service.
