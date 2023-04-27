"""Common CRUD utilities for API tests."""
from random import choice
from string import ascii_lowercase
from typing import Any

import shapely.wkb
from shapely import box
from sqlalchemy import select

from gerrydb_meta import crud, models, schemas
from gerrydb_meta.crud.column import COLUMN_TYPE_TO_VALUE_COLUMN
from gerrydb_meta.enums import ColumnKind, ColumnType


def create_column(
    ctx,
    path: str = "test",
    aliases: list[str] | None = None,
    col_kind: ColumnKind = ColumnKind.COUNT,
    col_type: ColumnType = ColumnType.INT,
) -> models.DataColumn:
    """Creates a column in a test context's namespace (direct CRUD)."""
    col, _ = crud.column.create(
        db=ctx.db,
        obj_in=schemas.ColumnCreate(
            canonical_path=path,
            description="Test column",
            kind=col_kind,
            type=col_type,
            aliases=aliases,
        ),
        obj_meta=ctx.meta,
        namespace=ctx.namespace,
    )
    return col


def create_locality(ctx) -> tuple[str, models.Locality]:
    """Creates a locality with a random name/canonical path (direct CRUD)."""
    path = "".join(choice(ascii_lowercase) for _ in range(10))
    locs, _ = crud.locality.create_bulk(
        db=ctx.db,
        objs_in=[
            schemas.LocalityCreate(
                canonical_path=path,
                parent_path=None,
                default_proj=None,
                name=f"random locality {path}",
            ),
        ],
        obj_meta=ctx.meta,
    )
    return locs[0], path


def create_geo_layer(ctx, path: str = "test") -> models.GeoLayer:
    """Creates a geographic layer in a test context's namespace (direct CRUD)."""
    layer, _ = crud.geo_layer.create(
        db=ctx.db,
        obj_in=schemas.GeoLayerCreate(
            path=path,
            description="Test geographic layer",
            source_url=None,
        ),
        obj_meta=ctx.meta,
        namespace=ctx.namespace,
    )
    return layer


def create_geo(ctx, path: str = "geo") -> models.Geography:
    """Creates a geography in a test context's namespace."""
    geo_import, _ = crud.geo_import.create(
        db=ctx.db,
        obj_meta=ctx.meta,
        namespace=ctx.namespace,
    )
    geo_objs = crud.geography.create_bulk(
        db=ctx.db,
        geo_import=geo_import,
        obj_meta=ctx.meta,
        namespace=ctx.namespace,
        objs_in=[
            schemas.GeographyCreate(
                path=path,
                geography=shapely.wkb.dumps(box(0, 0, 1, 1)),
            )
        ],
    )
    return geo_objs[0]


def get_column_values(ctx, col: models.DataColumn) -> dict[str, Any]:
    """Gets the values of a column in a test context's namespace."""
    value_col = COLUMN_TYPE_TO_VALUE_COLUMN[col.type]
    raw_values = ctx.db.execute(
        select(
            models.Geography.path,
            getattr(models.ColumnValue, value_col).label("value"),
        )
        .join(models.Geography, models.Geography.geo_id == models.ColumnValue.geo_id)
        .filter(
            models.ColumnValue.col_id == col.col_id,
            models.ColumnValue.valid_to.is_(None),
        ),
    )
    return {row.path: row.value for row in raw_values}
