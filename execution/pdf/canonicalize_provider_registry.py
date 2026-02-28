from __future__ import annotations

import importlib.metadata
from typing import Dict, List

from core.capability_registry import build_registry
from core.ordering import sort_dict


CANONICALIZER_PRIORITY: List[str] = [
    "qpdf",
    "mutool",
    "pikepdf",
]


def _provider_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except Exception:
        return "unknown"


def resolve_canonicalizer() -> Dict[str, str]:
    registry = build_registry()

    for provider in CANONICALIZER_PRIORITY:
        key = f"canonicalizer.{provider}"
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
            "reason": "no_canonicalizer_available",
        }
    )