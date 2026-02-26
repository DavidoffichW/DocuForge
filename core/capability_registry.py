from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List

from .capability_contract import Capability, CapabilityStatus, ProbeEvidence
from .capability_report import CapabilityReport, DegradationRecord


@dataclass(frozen=True)
class CapabilityRegistry:
    _capabilities: Dict[str, Capability]
    _report: CapabilityReport

    def get(self, name: str) -> Capability:
        if name not in self._capabilities:
            raise KeyError(f"Capability not registered: {name}")
        return self._capabilities[name]

    def all(self) -> Dict[str, Capability]:
        return dict(self._capabilities)

    def report(self) -> CapabilityReport:
        return self._report


def build_registry() -> CapabilityRegistry:
    capabilities: Dict[str, Capability] = {}
    degradations: List[DegradationRecord] = []
    probe_summary: Dict[str, Any] = {}

    probe_index = 0

    def next_probe_index() -> int:
        nonlocal probe_index
        idx = probe_index
        probe_index += 1
        return idx

    def probe_module(module_name: str) -> bool:
        try:
            importlib.import_module(module_name)
            return True
        except Exception:
            return False

    def register_capability(
        name: str,
        required_dependencies: List[str],
        fallback_strategy: str | None = None,
    ) -> None:
        idx = next_probe_index()
        missing = []
        for dep in required_dependencies:
            if not probe_module(dep):
                missing.append(dep)

        if not missing:
            status = CapabilityStatus.AVAILABLE
            degradation_reason = None
        elif fallback_strategy is not None:
            status = CapabilityStatus.DEGRADED
            degradation_reason = f"Missing dependencies: {missing}"
            degradations.append(
                DegradationRecord(
                    capability=name,
                    reason="degraded",
                    details={"missing_dependencies": missing},
                )
            )
        else:
            status = CapabilityStatus.UNAVAILABLE
            degradation_reason = f"Missing dependencies: {missing}"
            degradations.append(
                DegradationRecord(
                    capability=name,
                    reason="unavailable",
                    details={"missing_dependencies": missing},
                )
            )

        evidence = {
            "required_dependencies": list(required_dependencies),
            "missing_dependencies": missing,
        }

        capability = Capability(
            name=name,
            status=status,
            required_dependencies=list(required_dependencies),
            version_constraints=[],
            resource_requirements={},
            fallback_strategy=fallback_strategy,
            degradation_reason=degradation_reason,
            probe_evidence=ProbeEvidence(
                probe_index=idx,
                evidence=evidence,
            ),
        )

        capabilities[name] = capability

    # Core execution capabilities (v1)
    register_capability("pymupdf", ["fitz"], fallback_strategy=None)
    register_capability("pdfplumber", ["pdfplumber"], fallback_strategy="pymupdf")
    register_capability("python_runtime", [], fallback_strategy=None)

    # System resource probes (deterministic)
    idx = next_probe_index()
    cpu_count = os.cpu_count()
    probe_summary["cpu_count"] = cpu_count

    idx_mem = next_probe_index()
    try:
        import psutil  # optional
        total_mem = psutil.virtual_memory().total
        probe_summary["total_memory_bytes"] = total_mem
        mem_missing = []
    except Exception:
        probe_summary["total_memory_bytes"] = None
        mem_missing = ["psutil"]

    capabilities["system_resources"] = Capability(
        name="system_resources",
        status=CapabilityStatus.AVAILABLE,
        required_dependencies=[],
        version_constraints=[],
        resource_requirements={
            "cpu_count": cpu_count,
            "total_memory_bytes": probe_summary["total_memory_bytes"],
        },
        fallback_strategy=None,
        degradation_reason=None,
        probe_evidence=ProbeEvidence(
            probe_index=idx_mem,
            evidence={"missing_dependencies": mem_missing},
        ),
    )

    report = CapabilityReport(
        capabilities=capabilities,
        degradations=degradations,
        probe_summary=probe_summary,
    )

    return CapabilityRegistry(
        _capabilities=capabilities,
        _report=report,
    )