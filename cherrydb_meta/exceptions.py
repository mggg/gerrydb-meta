"""Global exceptions (largely related to CRUD operations/data integrity)."""


class CreateValueError(ValueError):
    """`ValueError` raised on failed object creation."""


class CreateConflictError(ValueError):
    """`ValueError` raised when object(s) already exist."""

    paths: list[str]

    def __init__(self, message: str, paths: list[str]):
        self.paths = paths
        super().__init__(message)
