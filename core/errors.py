from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional, Union


class ErrorCode(str, Enum):
    VALIDATION_ERROR = "VALIDATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    COLLISION = "COLLISION"
    INVALID_STATE = "INVALID_STATE"
    UNKNOWN_OPERATION = "UNKNOWN_OPERATION"
    CAPABILITY_UNAVAILABLE = "CAPABILITY_UNAVAILABLE"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class Failure:
    def __init__(
        self,
        code: Union[ErrorCode, str],
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ):
        if isinstance(code, ErrorCode):
            code_str = code.value
        elif isinstance(code, str) and code.strip():
            code_str = code
        else:
            raise ValueError("code must be ErrorCode or non-empty string")

        if not isinstance(message, str) or not message.strip():
            raise ValueError("message must be non-empty string")

        if details is not None and not isinstance(details, dict):
            raise ValueError("details must be dict if provided")

        self.code: str = code_str
        self.message: str = message
        self.details: Optional[Dict[str, Any]] = dict(details) if details is not None else None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "code": self.code,
            "details": self.details,
            "message": self.message,
        }
        return {k: payload[k] for k in sorted(payload.keys())}


def failure(code: Union[ErrorCode, str], message: str, details: Optional[Dict[str, Any]] = None) -> Failure:
    return Failure(code=code, message=message, details=details)


def validation_failure(message: str, details: Optional[Dict[str, Any]] = None) -> Failure:
    return Failure(code=ErrorCode.VALIDATION_ERROR, message=message, details=details)


class StorageError(Exception):
    def __init__(self, failure_obj: Failure):
        if not isinstance(failure_obj, Failure):
            raise ValueError("failure_obj must be Failure")
        super().__init__(failure_obj.message)
        self.failure: Failure = failure_obj


class StorageCollisionError(StorageError):
    pass