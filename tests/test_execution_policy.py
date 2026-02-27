from core.capability_registry import build_registry
from core.execution_policy import ExecutionPolicy


def test_policy_allows_available():
    registry = build_registry()
    policy = ExecutionPolicy(registry)

    decision = policy.require("python_runtime")
    assert decision.allowed is True


def test_policy_blocks_unknown():
    registry = build_registry()
    policy = ExecutionPolicy(registry)

    decision = policy.require("unknown_capability")
    assert decision.allowed is False


def test_provider_resolution_chain():
    registry = build_registry()
    policy = ExecutionPolicy(registry)

    res = policy.resolve_provider_chain("pdfplumber")
    assert res.status in {"OK", "BLOCKED"}