"""Common CRUD utilities for API tests."""
from gerrydb_meta import crud, models, schemas
from gerrydb_meta.enums import ColumnKind, ColumnType


def create_column(
    ctx, path: str = "test", aliases: list[str] | None = None
) -> models.DataColumn:
    """Creates a tally column in a test context's namespace (direct CRUD)."""
    col, _ = crud.column.create(
        db=ctx.db,
        obj_in=schemas.ColumnCreate(
            canonical_path=path,
            description="Test column",
            kind=ColumnKind.COUNT,
            type=ColumnType.INT,
            aliases=aliases,
        ),
        obj_meta=ctx.meta,
        namespace=ctx.namespace,
    )
    return col
