from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List


class CapabilityStatus(str, Enum):
    AVAILABLE = "available"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class Capability:
    name: str
    status: CapabilityStatus
    providers: List[str]

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "name": self.name,
            "providers": sorted(list(self.providers)),
            "status": self.status.value,
        }
        return {k: payload[k] for k in sorted(payload.keys())}