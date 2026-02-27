from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from core.capability_contract import CapabilityStatus
from core.capability_registry import CapabilityRegistry
from core.errors import ErrorCode, Failure, failure


@dataclass(frozen=True)
class Decision:
    allowed: bool
    failure: Optional[Failure] = None


@dataclass(frozen=True)
class ProviderResolution:
    status: str
    provider: Optional[str]
    failure: Optional[Failure]
    degradation: Optional[dict]


class ExecutionPolicy:
    def __init__(self, registry: CapabilityRegistry):
        self._registry = registry

    def require(self, capability_name: str) -> Decision:
        if not isinstance(capability_name, str) or not capability_name.strip():
            raise ValueError("capability_name must be a non-empty string")

        try:
            cap = self._registry.get(capability_name)
        except KeyError:
            f = failure(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "unknown capability",
                {"capability": capability_name},
            )
            return Decision(allowed=False, failure=f)

        if cap.status == CapabilityStatus.AVAILABLE:
            return Decision(allowed=True, failure=None)

        f = failure(
            ErrorCode.CAPABILITY_UNAVAILABLE,
            "capability unavailable",
            {"capability": capability_name, "status": cap.status.value},
        )
        return Decision(allowed=False, failure=f)

    def resolve_provider_chain(self, capability_name: str, preference: Optional[List[str]] = None) -> ProviderResolution:
        decision = self.require(capability_name)
        if not decision.allowed:
            return ProviderResolution(
                status="BLOCKED",
                provider=None,
                failure=decision.failure,
                degradation=None,
            )

        cap = self._registry.get(capability_name)
        providers = list(cap.providers)

        if preference:
            preferred = [p for p in preference if p in providers]
            if preferred:
                providers = preferred

        provider = providers[0] if providers else None

        return ProviderResolution(
            status="OK",
            provider=provider,
            failure=None,
            degradation=None,
        )