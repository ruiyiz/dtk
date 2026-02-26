from dtk.errors import (
    DtkConnectionError,
    DtkError,
    DtkFieldNotFoundError,
    DtkSchemaError,
    DtkSecurityNotFoundError,
    DtkTypeError,
    DtkValueError,
)


def test_error_hierarchy():
    assert issubclass(DtkValueError, DtkError)
    assert issubclass(DtkValueError, ValueError)
    assert issubclass(DtkTypeError, DtkError)
    assert issubclass(DtkTypeError, TypeError)
    assert issubclass(DtkConnectionError, DtkError)
    assert issubclass(DtkSchemaError, DtkError)
    assert issubclass(DtkSecurityNotFoundError, DtkError)
    assert issubclass(DtkFieldNotFoundError, DtkError)


def test_raise_dtk_error():
    try:
        raise DtkValueError("test")
    except DtkError:
        pass
    except Exception:
        raise AssertionError("DtkValueError should be caught by DtkError")
