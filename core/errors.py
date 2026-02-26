from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class ErrorCode(str, Enum):
    VALIDATION_ERROR = "VALIDATION_ERROR"
    CAPABILITY_UNAVAILABLE = "CAPABILITY_UNAVAILABLE"
    POLICY_BLOCKED = "POLICY_BLOCKED"
    EXECUTION_FAILED = "EXECUTION_FAILED"
    STORAGE_ERROR = "STORAGE_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"


@dataclass(frozen=True)
class Failure:
    code: str
    message: str
    details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details is not None:
            d["details"] = self.details
        return d


def failure(code: ErrorCode | str, message: str, details: Optional[Dict[str, Any]] = None) -> Failure:
    if isinstance(code, ErrorCode):
        code_str = code.value
    else:
        code_str = str(code)
    if not code_str:
        raise ValueError("code must be non-empty")
    if not isinstance(message, str) or not message.strip():
        raise ValueError("message must be a non-empty string")
    if details is not None and not isinstance(details, dict):
        raise ValueError("details must be a dict if provided")
    return Failure(code=code_str, message=message, details=details)


def validation_failure(message: str, details: Optional[Dict[str, Any]] = None) -> Failure:
    return failure(ErrorCode.VALIDATION_ERROR, message, details)


def capability_failure(capability_name: str, message: str, details: Optional[Dict[str, Any]] = None) -> Failure:
    if not isinstance(capability_name, str) or not capability_name.strip():
        raise ValueError("capability_name must be a non-empty string")
    d = {} if details is None else dict(details)
    d["capability"] = capability_name
    return failure(ErrorCode.CAPABILITY_UNAVAILABLE, message, d)


def policy_blocked(message: str, details: Optional[Dict[str, Any]] = None) -> Failure:
    return failure(ErrorCode.POLICY_BLOCKED, message, details)


def execution_failed(message: str, details: Optional[Dict[str, Any]] = None) -> Failure:
    return failure(ErrorCode.EXECUTION_FAILED, message, details)