# DocuForge

DocuForge is a polished local PDF workspace for viewing documents, organizing
pages, and saving immutable document versions. Its focused browser interface
uses a compact dark ruby and silver-grey visual system with icon-led document
and page controls. The application combines a
FastAPI service, SQLite metadata, managed content-addressed storage, pikepdf for
PDF transformations, and PDF.js for the browser viewer.

Version 0.1.0 supports:

- PDF import and validation with a 256 MiB default upload limit;
- text selection, document search, PDF links, page navigation, and zoom;
- lazy page viewing and bounded, on-demand thumbnail rendering;
- live page reorder, rotation, deletion, extraction, undo, and redo without
  rebuilding the PDF until Save;
- immutable Save versions, downloads, and restart recovery;
- HTTP byte-range delivery for stored PDF versions; and
- fidelity checks that block transformations which would silently lose
  supported links, bookmarks, annotations, form fields, or page content.

The Save command persists the current page plan as a new immutable version.
Download PDF downloads the current saved version; with unsaved changes, it
becomes Save and download and materializes a new version first. Tooltips and
accessible names identify icon-only controls. Extract downloads only the
selected pages and does not alter the working document.

Annotations, comments, drawing, OCR, existing-content text or image editing,
shared-server deployment, and collaborative workspaces are not implemented.
DocuForge is a page-operations editor intended for trusted single-user use on
the local machine, not a general replacement for a full PDF authoring suite.

## Requirements

- CPython 3.11 or newer
- A current desktop browser supported by PDF.js (automated browser qualification
  uses Chromium)

## Install and run

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m docuforge
```

Open <http://127.0.0.1:8000>. The server intentionally rejects non-loopback
bind addresses and non-local HTTP Host values in this milestone. Browser
mutations must be same-origin. Local non-browser API clients must opt in with
`X-DocuForge-Client: local-api`; they must not send browser Origin or Fetch
Metadata headers. Set `DOCUFORGE_WORKSPACE` or pass `--workspace PATH` to
choose the managed storage location.

The original imported bytes are retained unchanged. Save creates a new
validated version instead of overwriting the source. A page operation that
would create a known dangling internal link is rejected rather than silently
degrading the document.

Fidelity validation compares page content, navigation, named-destination view
parameters, supported annotation actions, forms, and resource structure.
Unsupported action types block the affected transformation. Decoded
image-resource hashing is capped at 1 MiB per semantic inspection; larger or
unsupported image encodings are compared by their declared structure rather
than by decoding every pixel.

The interface defaults to its dark theme and also provides a persistent light
theme. Motion is limited to short state and continuity cues and follows the
operating system's reduced-motion preference.

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
under Apache-2.0. Selected Lucide icon paths are redistributed under ISC and,
where derived from Feather, MIT terms. See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) and the license texts retained
with the vendored assets.
