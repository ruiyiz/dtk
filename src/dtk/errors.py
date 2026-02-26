class DtkError(Exception):
    pass


class DtkValueError(DtkError, ValueError):
    pass


class DtkTypeError(DtkError, TypeError):
    pass


class DtkConnectionError(DtkError):
    pass


class DtkSchemaError(DtkError):
    pass


class DtkSecurityNotFoundError(DtkError):
    pass


class DtkFieldNotFoundError(DtkError):
    pass
