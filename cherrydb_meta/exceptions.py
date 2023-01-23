"""Global exceptions (largely related to CRUD operations/data integrity)."""

class CreateValueError(ValueError):
    """`ValueError` raised on failed object creation."""