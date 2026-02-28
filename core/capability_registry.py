from __future__ import annotations

import importlib
import shutil
from typing import Dict

from core.capability_contract import Capability, CapabilityStatus
from core.capability_report import CapabilityReport


def _probe_import(module_name: str) -> bool:
    if not isinstance(module_name, str) or not module_name.strip():
        raise ValueError("module_name must be a non-empty string")
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


def _probe_binary(binary_name: str) -> bool:
    if not isinstance(binary_name, str) or not binary_name.strip():
        raise ValueError("binary_name must be a non-empty string")
    return shutil.which(binary_name) is not None


class CapabilityRegistry:
    def __init__(self, capabilities: Dict[str, Capability]):
        if not isinstance(capabilities, dict):
            raise ValueError("capabilities must be a dict")
        self._capabilities = dict(capabilities)

    def get(self, name: str) -> Capability:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
        if name not in self._capabilities:
            raise KeyError(name)
        return self._capabilities[name]

    def all(self) -> Dict[str, Capability]:
        return {k: self._capabilities[k] for k in sorted(self._capabilities.keys())}

    def report(self) -> CapabilityReport:
        return CapabilityReport(schema_version="v1", capabilities=self.all())

    def to_dict(self) -> Dict[str, object]:
        return self.report().to_dict()


def build_registry() -> CapabilityRegistry:
    caps: Dict[str, Capability] = {}

    caps["python_runtime"] = Capability(
        name="python_runtime",
        status=CapabilityStatus.AVAILABLE,
        providers=["builtin"],
    )

    pymupdf_ok = _probe_import("fitz")
    caps["pymupdf"] = Capability(
        name="pymupdf",
        status=CapabilityStatus.AVAILABLE if pymupdf_ok else CapabilityStatus.DEGRADED,
        providers=["pymupdf"] if pymupdf_ok else [],
    )
    caps["pdf_engine.pymupdf"] = Capability(
        name="pdf_engine.pymupdf",
        status=CapabilityStatus.AVAILABLE if pymupdf_ok else CapabilityStatus.DEGRADED,
        providers=["pymupdf"] if pymupdf_ok else [],
    )

    pypdf_ok = _probe_import("pypdf")
    caps["pdf_engine.pypdf"] = Capability(
        name="pdf_engine.pypdf",
        status=CapabilityStatus.AVAILABLE if pypdf_ok else CapabilityStatus.DEGRADED,
        providers=["pypdf"] if pypdf_ok else [],
    )

    pdfplumber_ok = _probe_import("pdfplumber")
    caps["pdfplumber"] = Capability(
        name="pdfplumber",
        status=CapabilityStatus.AVAILABLE if pdfplumber_ok else CapabilityStatus.DEGRADED,
        providers=["pdfplumber"] if pdfplumber_ok else [],
    )
    caps["table_engine.pdfplumber"] = Capability(
        name="table_engine.pdfplumber",
        status=CapabilityStatus.AVAILABLE if pdfplumber_ok else CapabilityStatus.DEGRADED,
        providers=["pdfplumber"] if pdfplumber_ok else [],
    )

    camelot_ok = _probe_import("camelot")
    caps["table_engine.camelot"] = Capability(
        name="table_engine.camelot",
        status=CapabilityStatus.AVAILABLE if camelot_ok else CapabilityStatus.DEGRADED,
        providers=["camelot"] if camelot_ok else [],
    )

    qpdf_ok = _probe_binary("qpdf")
    caps["canonicalizer.qpdf"] = Capability(
        name="canonicalizer.qpdf",
        status=CapabilityStatus.AVAILABLE if qpdf_ok else CapabilityStatus.DEGRADED,
        providers=["qpdf"] if qpdf_ok else [],
    )

    mutool_ok = _probe_binary("mutool")
    caps["canonicalizer.mutool"] = Capability(
        name="canonicalizer.mutool",
        status=CapabilityStatus.AVAILABLE if mutool_ok else CapabilityStatus.DEGRADED,
        providers=["mutool"] if mutool_ok else [],
    )

    pikepdf_ok = _probe_import("pikepdf")
    caps["canonicalizer.pikepdf"] = Capability(
        name="canonicalizer.pikepdf",
        status=CapabilityStatus.AVAILABLE if pikepdf_ok else CapabilityStatus.DEGRADED,
        providers=["pikepdf"] if pikepdf_ok else [],
    )

    return CapabilityRegistry(caps)