# DocuForge

DocuForge is a local, browser-based PDF workspace for opening documents,
organizing pages, and saving immutable document versions. The editor combines a
FastAPI service, SQLite metadata, managed content-addressed storage, pikepdf for
PDF transformations, and PDF.js for the browser viewer.

The current editor foundation supports:

- PDF import and validation with a 256 MiB default upload limit;
- text selection, document search, PDF links, page navigation, and zoom;
- lazy page viewing and bounded, on-demand thumbnail rendering;
- page reorder, rotation, deletion, extraction, undo, and redo;
- immutable Save versions, downloads, and restart recovery;
- HTTP byte-range delivery for stored PDF versions; and
- fidelity checks that block transformations which would silently lose
  supported links, bookmarks, annotations, form fields, or page content.

Annotations, OCR, existing-content editing, shared-server deployment, and
collaborative workspaces are not implemented. DocuForge is intended for trusted
single-user use on the local machine.

## Requirements

- CPython 3.11 or newer
- A current Chromium, Firefox, or Edge browser

## Install and run

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m docuforge
```

Open <http://127.0.0.1:8000>. The server intentionally rejects non-loopback
bind addresses in this milestone. Set `DOCUFORGE_WORKSPACE` or pass
`--workspace PATH` to choose the managed storage location.

The original imported bytes are retained unchanged. Save creates a new
validated version instead of overwriting the source. A page operation that
would create a known dangling internal link is rejected rather than silently
degrading the document.

Fidelity validation compares page content, navigation, annotations, forms, and
resource structure. Decoded image-resource hashing is capped at 1 MiB per
semantic inspection; larger or unsupported image encodings are compared by
their declared structure rather than by decoding every pixel.

## Development

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
```

Browser tests also require Chromium:

```powershell
.\.venv\Scripts\python.exe -m playwright install chromium
.\.venv\Scripts\python.exe -m pytest -q -m browser
```

## Licensing

DocuForge source is available under the MIT License. PDF.js is redistributed
under Apache-2.0. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) and the
license retained with the vendored PDF.js distribution.
