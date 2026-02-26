from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class CapabilityStatus(str, Enum):
    AVAILABLE = "available"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class ProbeEvidence:
    probe_index: int
    evidence: Dict[str, Any]


@dataclass(frozen=True)
class Capability:
    name: str
    status: CapabilityStatus
    required_dependencies: List[str] = field(default_factory=list)
    version_constraints: List[str] = field(default_factory=list)
    resource_requirements: Dict[str, Any] = field(default_factory=dict)
    fallback_strategy: Optional[str] = None
    degradation_reason: Optional[str] = None
    probe_evidence: Optional[ProbeEvidence] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "required_dependencies": list(self.required_dependencies),
            "version_constraints": list(self.version_constraints),
            "resource_requirements": dict(self.resource_requirements),
            "fallback_strategy": self.fallback_strategy,
            "degradation_reason": self.degradation_reason,
            "probe_evidence": {
                "probe_index": self.probe_evidence.probe_index,
                "evidence": dict(self.probe_evidence.evidence),
            }
            if self.probe_evidence
            else None,
        }