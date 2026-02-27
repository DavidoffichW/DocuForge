from core.errors import failure, validation_failure, ErrorCode


def test_failure_structure():
    f = failure(ErrorCode.VALIDATION_ERROR, "bad input", {"x": 1})
    d = f.to_dict()

    assert d["code"] == ErrorCode.VALIDATION_ERROR.value
    assert d["message"] == "bad input"
    assert d["details"]["x"] == 1


def test_validation_failure_shortcut():
    f = validation_failure("invalid")
    assert f.code == ErrorCode.VALIDATION_ERROR.value