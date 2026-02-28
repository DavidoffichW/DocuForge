from __future__ import annotations

from typing import Any, Dict

from execution.validation.contracts import OPERATION_PARAM_CONTRACT


def validate_operation_params(operation: str, params: Dict[str, Any]) -> None:
    if not isinstance(operation, str) or not operation.strip():
        raise ValueError("operation must be non-empty string")

    if not isinstance(params, dict):
        raise ValueError("params must be dict")

    if operation not in OPERATION_PARAM_CONTRACT:
        raise ValueError("unknown operation")

    allowed = OPERATION_PARAM_CONTRACT[operation]

    for k in params.keys():
        if k not in allowed:
            raise ValueError(f"unknown parameter: {k}")