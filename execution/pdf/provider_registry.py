from __future__ import annotations

import importlib.metadata
from typing import Dict, List, Tuple

from core.capability_registry import build_registry
from core.ordering import sort_dict


PDF_PROVIDER_PRIORITY: List[str] = [
    "pymupdf",
    "pypdf",
]


def _provider_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except Exception:
        return "unknown"


def resolve_pdf_provider(operation: str) -> Dict[str, str]:
    registry = build_registry()

    for provider in PDF_PROVIDER_PRIORITY:
        key = f"pdf_engine.{provider}"
        if registry.get(key) == "available":
            return sort_dict(
                {
                    "provider": provider,
                    "provider_version": _provider_version(provider),
                    "degraded": False,
                }
            )

    return sort_dict(
        {
            "provider": "",
            "provider_version": "",
            "degraded": True,
            "reason": "no_pdf_provider_available",
        }
    )