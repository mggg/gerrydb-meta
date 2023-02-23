"""Global exceptions (largely related to CRUD operations/data integrity)."""
from dataclasses import dataclass


class CreateValueError(ValueError):
    """`ValueError` raised on failed object creation."""


class BulkCreateError(ValueError):
    """`ValueError` raised when object(s) already exist."""

    paths: list[str]

    def __init__(self, message: str, paths: list[str]):
        self.paths = paths
        super().__init__(message)


@dataclass
class ColumnValueTypeError(ValueError):
    """`ValueError` raised when column value(s) do not match the column type."""

    errors: list[str]
