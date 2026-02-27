from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from core.capability_contract import Capability


@dataclass(frozen=True)
class CapabilityReport:
    schema_version: str
    capabilities: Dict[str, Capability]

    def to_dict(self) -> Dict[str, object]:
        caps: List[Dict[str, object]] = []
        for name in sorted(self.capabilities.keys()):
            caps.append(self.capabilities[name].to_dict())
        payload: Dict[str, object] = {
            "capabilities": caps,
            "schema_version": self.schema_version,
        }
        return {k: payload[k] for k in sorted(payload.keys())}