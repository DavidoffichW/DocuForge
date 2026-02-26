from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .capability_contract import Capability, CapabilityStatus
from .capability_registry import CapabilityRegistry
from .capability_report import DegradationRecord
from .errors import Failure, capability_failure, policy_blocked


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    failure: Optional[Failure] = None
    degradation: Optional[DegradationRecord] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "failure": self.failure.to_dict() if self.failure else None,
            "degradation": self.degradation.to_dict() if self.degradation else None,
        }


@dataclass(frozen=True)
class ProviderResolution:
    status: str
    provider: Optional[str]
    failure: Optional[Failure]
    degradation: Optional[DegradationRecord]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "provider": self.provider,
            "failure": self.failure.to_dict() if self.failure else None,
            "degradation": self.degradation.to_dict() if self.degradation else None,
        }


class ExecutionPolicy:
    def __init__(self, registry: CapabilityRegistry):
        self._registry = registry

    def require(self, capability_name: str) -> PolicyDecision:
        if not isinstance(capability_name, str) or not capability_name.strip():
            return PolicyDecision(
                allowed=False,
                failure=policy_blocked(
                    "capability_name must be a non-empty string",
                    {"capability": capability_name},
                ),
                degradation=None,
            )

        try:
            cap = self._registry.get(capability_name)
        except KeyError:
            return PolicyDecision(
                allowed=False,
                failure=capability_failure(
                    capability_name,
                    "Capability not registered",
                    {"capability": capability_name},
                ),
                degradation=None,
            )

        if cap.status == CapabilityStatus.AVAILABLE:
            return PolicyDecision(allowed=True, failure=None, degradation=None)

        if cap.status == CapabilityStatus.DEGRADED:
            return PolicyDecision(
                allowed=True,
                failure=None,
                degradation=DegradationRecord(
                    capability=capability_name,
                    reason="degraded",
                    details={"degradation_reason": cap.degradation_reason},
                ),
            )

        return PolicyDecision(
            allowed=False,
            failure=capability_failure(
                capability_name,
                "Capability unavailable",
                {"degradation_reason": cap.degradation_reason},
            ),
            degradation=DegradationRecord(
                capability=capability_name,
                reason="unavailable",
                details={"degradation_reason": cap.degradation_reason},
            ),
        )

    def resolve_provider_chain(
        self,
        capability_name: str,
        preference: Optional[List[str]] = None,
    ) -> ProviderResolution:
        if preference is not None and any((not isinstance(p, str) or not p.strip()) for p in preference):
            return ProviderResolution(
                status="BLOCKED",
                provider=None,
                failure=policy_blocked("provider_preference must be list[str] of non-empty strings", {}),
                degradation=None,
            )

        providers = self._build_provider_chain(capability_name)

        if preference:
            pref_set = {p for p in preference}
            filtered = [p for p in providers if p in pref_set]
            providers = filtered if filtered else providers

        for p in providers:
            decision = self.require(p)
            if decision.allowed:
                return ProviderResolution(
                    status="OK",
                    provider=p,
                    failure=None,
                    degradation=decision.degradation,
                )

        return ProviderResolution(
            status="BLOCKED",
            provider=None,
            failure=capability_failure(
                capability_name,
                "No provider available for capability chain",
                {"providers": providers},
            ),
            degradation=DegradationRecord(
                capability=capability_name,
                reason="unavailable",
                details={"providers": providers},
            ),
        )

    def _build_provider_chain(self, capability_name: str) -> List[str]:
        base = [capability_name]
        try:
            cap = self._registry.get(capability_name)
        except KeyError:
            return base

        if cap.fallback_strategy and isinstance(cap.fallback_strategy, str) and cap.fallback_strategy.strip():
            base.append(cap.fallback_strategy.strip())
        return base