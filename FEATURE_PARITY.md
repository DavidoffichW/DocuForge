# PDF workspace parity

DocuForge v0.2.0 combines its versioned PDF editor with the useful structured
extraction workflows previously released in PDF ETL Toolkit.

| Capability | DocuForge v0.2.0 behavior |
| --- | --- |
| PDF viewing | PDF.js viewer with search, selection, links, thumbnails, zoom, and range delivery. |
| Page rearrangement | Pending page map supports reorder and rotation before an immutable save. |
| Page removal | Deletion creates a new version; the source version is retained. |
| Page extraction | Selected pages are a downloadable artifact and do not mutate the source. |
| Multiple-document merge | Ordered managed versions are merged by the existing pikepdf engine and fidelity-verified before a new document is committed. |
| Table detection | Each page uses pdfplumber first and the deterministic text-grid fallback only when needed. |
| Table selection | Whole table, row, column, and cell selections are supported. |
| CSV export | Stable provenance schema, deterministic serialization, and spreadsheet-formula neutralization. |
| JSONL export | Stable provenance schema with original extracted text. |
| XLSX export | Valid workbook with literal formula-like text and normalized metadata. |
| ZIP export | Fixed ordering, timestamps, permissions, and member metadata. |
| Batch extraction | Multiple local files or a browser-selected folder, fixed table selection, stable ordering, and per-file failure isolation. |
| Resource bounds | Separate upload, merge, batch-file, total-byte, file-count, and page limits. |
| Security | Loopback Host/origin boundary, bounded uploads, no server-path browser API, and spreadsheet safeguards. |
| Error handling | Controlled failures do not overwrite source versions, create a successful merge record, or discard successful batch results. |

## Deliberate interface differences

The predecessor's Streamlit server-directory field is not retained. DocuForge
uses browser file selection, with optional folder selection where the browser
supports it, so a page cannot request arbitrary server-side filesystem paths.
Merge sources become managed immutable versions before processing. These are
intentional security and provenance improvements rather than missing parity.

OCR remains unsupported. Table quality depends on the PDF text layer and page
layout. PDF transformations with unsupported interactive structures are
rejected instead of being silently degraded.
