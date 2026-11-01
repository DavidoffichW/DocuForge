# Third-party notices

DocuForge is MIT-licensed. Its runtime dependencies retain their own licenses.

## PDF.js 4.10.38

The browser workspace includes files from Mozilla PDF.js 4.10.38
(`pdfjs-dist`). PDF.js is Copyright Mozilla Foundation and contributors and is
licensed under the Apache License, Version 2.0. The bundled license text is in
`docuforge/editor/vendor/pdfjs/LICENSE`.

Source and release information: <https://github.com/mozilla/pdf.js/releases/tag/v4.10.38>

## Lucide Icons 1.65.0

The browser workspace embeds selected SVG paths from Lucide Icons 1.65.0.
Lucide is licensed under the ISC License; icons derived from Feather retain
the MIT terms reproduced in the upstream license. The complete license text
is retained at `docuforge/editor/vendor/lucide/LICENSE`.

Source and release information: <https://github.com/lucide-icons/lucide/releases/tag/1.65.0>

## pikepdf and qpdf

DocuForge uses pikepdf under the Mozilla Public License 2.0. Official pikepdf
wheels include qpdf and other native dependencies. Their license files are
distributed in the installed wheel and remain applicable.

- pikepdf: <https://github.com/pikepdf/pikepdf>
- qpdf: <https://github.com/qpdf/qpdf>

## pypdf

DocuForge uses pypdf under the BSD 3-Clause license for structural inspection
and fidelity verification.

Source: <https://github.com/py-pdf/pypdf>

## pdfplumber and PDFMiner.six

DocuForge uses pdfplumber and PDFMiner.six under the MIT License for table,
text, and page-geometry extraction. Their source distributions and wheels
retain the applicable license files.

- pdfplumber: <https://github.com/jsvine/pdfplumber>
- PDFMiner.six: <https://github.com/pdfminer/pdfminer.six>

pdfplumber currently installs pypdfium2 as a runtime dependency. pypdfium2 is
available under BSD-3-Clause or Apache-2.0 terms and its wheel carries PDFium
and bundled third-party license notices. DocuForge does not use PDFium as its
editor or transformation authority.

Source: <https://github.com/pypdfium2-team/pypdfium2>

## openpyxl

DocuForge uses openpyxl under the MIT License to create XLSX table exports.

Source: <https://foss.heptapod.net/openpyxl/openpyxl>

The dependency inventory must be rechecked before each public release. This
notice is not a substitute for the license files distributed with installed
dependencies.
