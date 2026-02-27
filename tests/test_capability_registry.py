import importlib
import types

import pytest

from core.capability_registry import build_registry


def test_registry_builds_and_reports():
    registry = build_registry()
    report = registry.report().to_dict()

    assert "capabilities" in report
    assert "schema_version" in report


def test_registry_contains_expected_capabilities():
    registry = build_registry()
    caps = registry.all()

    assert "python_runtime" in caps
    assert "pymupdf" in caps
    assert "pdfplumber" in caps


def test_missing_module_degrades(monkeypatch):
    original_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name == "pdfplumber":
            raise ImportError("simulated missing")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", fake_import)

    registry = build_registry()
    cap = registry.get("pdfplumber")

    assert cap.status.value in {"degraded", "unavailable"}