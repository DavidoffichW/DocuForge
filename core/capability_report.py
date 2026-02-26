from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from .capability_contract import Capability


@dataclass(frozen=True)
class DegradationRecord:
    capability: str
    reason: str
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "capability": self.capability,
            "reason": self.reason,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class CapabilityReport:
    capabilities: Dict[str, Capability]
    degradations: List[DegradationRecord] = field(default_factory=list)
    probe_summary: Dict[str, Any] = field(default_factory=dict)
    schema_version: str = "capability_report_v1"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "capabilities": {
                name: cap.to_dict()
                for name, cap in sorted(self.capabilities.items())
            },
            "degradations": [d.to_dict() for d in self.degradations],
            "probe_summary": dict(self.probe_summary),
        }