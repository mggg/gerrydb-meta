"""Global exceptions (largely related to CRUD operations/data integrity)."""
from dataclasses import dataclass


class GerryValueError(ValueError):
    """Base GerryDB `ValueError`."""


class CreateValueError(GerryValueError):
    """`ValueError` raised on failed object creation."""


class BulkCreateError(GerryValueError):
    """`ValueError` raised when object(s) already exist."""

    paths: list[str] | None

    def __init__(self, message: str, paths: list[str] | None = None):
        self.paths = paths
        super().__init__(message)


class BulkPatchError(GerryValueError):
    """`ValueError` raised when patched object(s) do not exist."""

    paths: list[str] | None

    def __init__(self, message: str, paths: list[str] | None = None):
        self.paths = paths
        super().__init__(message)


@dataclass
class ColumnValueTypeError(GerryValueError):
    """`ValueError` raised when column value(s) do not match the column type."""

    errors: list[str]
