# DocuForge

> A local PDF workspace for organizing documents, preserving versions, and extracting tables.

DocuForge gives you one place to inspect PDFs, organize their pages, combine
documents, and turn detected tables into structured files. It runs on your
machine and does not send documents to a hosted service.

## What it does

- View PDFs with search, text selection, links, zoom, and page thumbnails
- Reorder, rotate, delete, and extract pages
- Undo and redo page changes before saving
- Save changes as immutable document versions
- Reopen saved versions after restarting the app
- Merge saved documents or newly selected PDFs in a chosen order
- Refuse transformations that would discard supported document structures
- Detect text-based tables and export CSV, JSONL, XLSX, or a reproducible ZIP
- Run the same table extraction over a bounded batch of uploaded PDFs

The original PDF is never overwritten.

## Install

DocuForge requires Python 3.11 or newer and a current desktop browser.

### Windows

```powershell
git clone https://github.com/DavidoffichW/DocuForge.git
cd DocuForge
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install .
.\.venv\Scripts\python.exe -m docuforge
```

### macOS and Linux

```bash
git clone https://github.com/DavidoffichW/DocuForge.git
cd DocuForge
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
python -m docuforge
```

Open <http://127.0.0.1:8000>. Keep the terminal running while you use the app.
Press `Ctrl+C` to stop it.

DocuForge stores its database and managed PDF versions in a local workspace.
To put that workspace somewhere specific:

```bash
python -m docuforge --workspace /path/to/workspace
```

You can also set the `DOCUFORGE_WORKSPACE` environment variable.

## Using it

Open a PDF, then use the page rail to select and arrange pages. Changes appear
in the main viewer immediately, but the imported file stays untouched until
you choose **Save**.

Open the document library to merge PDFs. You can import several local files or
add existing managed versions, arrange the source order, and create a new
document. Merge uses the same structural fidelity checks as page editing. A
blocked merge leaves every source document untouched and does not create a
partially successful result.

**Save** creates a new version in the workspace. **Download PDF** downloads the
current saved version. If you have unsaved changes, the button becomes
**Save and download** and creates a version first.

**Extract** downloads the selected pages as a separate PDF. It does not remove
them from the document.

Open **Table tools** from the view bar to inspect tables in the current saved
version. Select a candidate, choose the whole table or a row, column, or cell,
then download the structured result. JSONL keeps the extracted text unchanged;
CSV and XLSX prefix formula-like values so common spreadsheet applications do
not execute PDF text as a formula.

The batch tool accepts multiple uploaded PDFs and, in browsers that support it,
a locally selected folder. It never accepts a server filesystem path. By
default it allows 100 files, 50 MiB per file, 250 MiB in total, and 2,000
processed pages. A bad file is reported without discarding successful results
from the rest of the batch. These limits are deliberately conservative controls
for the in-memory extraction path, not performance claims.

Merge is separately bounded to 50 managed source versions, 512 MiB of source
bytes, and 5,000 pages by default.

Normal document uploads are limited to 256 MiB by default. Table extraction is
text-based; scanned image-only pages need OCR, which DocuForge does not provide.

## PDF fidelity

PDFs can contain more than pages and text. They may also have links, bookmarks,
forms, annotations, named destinations, and document-level references.

DocuForge checks the structures it knows how to preserve before accepting a
saved result. If an operation would leave a dangling link or lose supported
interactive content, it fails instead of quietly producing a damaged PDF.

This is deliberately conservative. It does not mean every PDF feature is
supported or that every document can be transformed.

## Local by design

DocuForge is a single-user desktop-style app served from a local FastAPI
process. It only binds to loopback addresses and rejects non-local hosts and
cross-origin browser mutations.

It has no accounts or remote-access security model. Do not expose it to a
network or run it as a shared server.

## Not supported

- Creating highlights, comments, drawings, or other annotations
- OCR for scanned documents
- Editing text or images already inside a PDF page
- Collaboration or multi-user workspaces
- General-purpose remote deployment

DocuForge is a page editor, not a full PDF authoring suite.

## How it works

- [FastAPI](https://fastapi.tiangolo.com/) serves the editor and local API
- [PDF.js](https://mozilla.github.io/pdf.js/) renders documents in the browser
- [pikepdf](https://pikepdf.readthedocs.io/) applies PDF transformations
- [pypdf](https://pypdf.readthedocs.io/) performs independent structural checks
- [pdfplumber](https://github.com/jsvine/pdfplumber) detects tables and provides
  the text-and-geometry fallback
- [openpyxl](https://openpyxl.readthedocs.io/) writes XLSX exports
- SQLite stores document, version, session, and operation metadata

PDF bytes are kept in managed content-addressed storage. Saved versions are
immutable, so a failed save cannot replace a valid earlier version.

The table and batch workflows were consolidated from the earlier PDF ETL
Toolkit after their behavior was reimplemented and qualified in this workspace.
The [feature-parity record](FEATURE_PARITY.md) documents the accepted interface
differences and remaining limits.

## Development

Create a virtual environment, then install the project and development tools:

```bash
python -m pip install -e .
python -m pip install -r requirements-dev.txt
```

Run the backend tests and lint checks:

```bash
python -m pytest -q -m "not browser"
python -m ruff check .
```

Browser tests use Chromium:

```bash
python -m playwright install chromium
python -m pytest -q -m browser
```

## License

MIT © DavidoffichW

PDF.js, Lucide, pikepdf, qpdf, pypdf, and other dependencies retain their own
licenses. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) and the license
files shipped with the vendored browser assets.
