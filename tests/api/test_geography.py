"""Tests for GerryDB REST API geography endpoints."""
from http import HTTPStatus

import msgpack
import shapely.wkb
from shapely import box
from shapely.geometry import Point

from gerrydb_meta import crud, schemas
from gerrydb_meta.enums import ScopeType
from gerrydb_meta.main import API_PREFIX

GEOS_ROOT = f"{API_PREFIX}/geographies"


def geo_import_id(ctx) -> str:
    """Creates a GeoImport ID (direct CRUD)."""
    geo_import, _ = crud.geo_import.create(
        db=ctx.db,
        obj_meta=ctx.meta,
        namespace=ctx.namespace,
    )
    return geo_import.uuid.hex


def headers(ctx) -> str:
    """Generates headers for POST requests."""
    return {
        "content-type": "application/msgpack",
        "x-gerrydb-geo-import-id": geo_import_id(ctx),
    }


def test_api_geography_create_read(ctx_public_namespace_read_write):
    ctx = ctx_public_namespace_read_write
    namespace = ctx.namespace.path

    unit_box = box(0, 0, 1, 1)
    box_wkb = shapely.wkb.dumps(unit_box)
    create_response = ctx.client.post(
        f"{GEOS_ROOT}/{namespace}",
        headers=headers(ctx),
        content=msgpack.dumps([{"path": "box", "geography": box_wkb}]),
    )
    assert create_response.status_code == HTTPStatus.CREATED, msgpack.loads(
        create_response.content
    )

    create_body = schemas.Geography(**msgpack.loads(create_response.content)[0])
    assert create_body.path == "box"
    assert create_body.internal_point is None
    assert shapely.wkb.loads(create_body.geography) == unit_box

    read_response = ctx.client.get(f"{GEOS_ROOT}/{namespace}/box")
    assert read_response.status_code == HTTPStatus.OK, read_response.json()

    read_body = schemas.GeographyMeta(**read_response.json())
    assert read_body.path == "box"
    assert read_body.namespace == namespace
    assert read_body.meta == create_body.meta


def test_api_geography_create_all(ctx_public_namespace_read_write):
    ctx = ctx_public_namespace_read_write
    namespace = ctx.namespace.path

    unit_box = box(0, 0, 1, 1)
    box_wkb = shapely.wkb.dumps(unit_box)
    create_response = ctx.client.post(
        f"{GEOS_ROOT}/{namespace}",
        headers=headers(ctx),
        content=msgpack.dumps([{"path": "box", "geography": box_wkb}]),
    )
    assert create_response.status_code == HTTPStatus.CREATED, msgpack.loads(
        create_response.content
    )
    create_body = schemas.Geography(**msgpack.loads(create_response.content)[0])

    all_response = ctx.client.get(f"{GEOS_ROOT}/{namespace}")
    assert all_response.status_code == HTTPStatus.OK, all_response.json()
    assert len(all_response.json()) == 1

    all_body = schemas.GeographyMeta(**all_response.json()[0])
    assert all_body.path == "box"
    assert all_body.namespace == namespace
    assert all_body.meta == create_body.meta


def test_api_geography_create__internal_point(ctx_public_namespace_read_write):
    ctx = ctx_public_namespace_read_write
    namespace = ctx.namespace.path

    unit_box = box(0, 0, 1, 1)
    internal_point = Point(0.5, 0.5)
    create_response = ctx.client.post(
        f"{GEOS_ROOT}/{namespace}",
        headers=headers(ctx),
        content=msgpack.dumps(
            [
                {
                    "path": "box",
                    "geography": shapely.wkb.dumps(unit_box),
                    "internal_point": shapely.wkb.dumps(internal_point),
                }
            ]
        ),
    )
    assert create_response.status_code == HTTPStatus.CREATED, msgpack.loads(
        create_response.content
    )

    create_body = schemas.Geography(**msgpack.loads(create_response.content)[0])
    assert create_body.path == "box"
    assert create_body.namespace == namespace
    assert shapely.wkb.loads(create_body.internal_point) == internal_point
    assert shapely.wkb.loads(create_body.geography) == unit_box


def test_api_geography_create__missing_geos(ctx_public_namespace_read_write):
    ctx = ctx_public_namespace_read_write
    namespace = ctx.namespace.path

    create_response = ctx.client.post(
        f"{GEOS_ROOT}/{namespace}",
        headers=headers(ctx),
        content=msgpack.dumps(
            [{"path": "box", "geography": None, "internal_point": None}]
        ),
    )
    assert create_response.status_code == HTTPStatus.CREATED, msgpack.loads(
        create_response.content
    )

    create_body = schemas.Geography(**msgpack.loads(create_response.content)[0])
    assert create_body.internal_point is None
    assert create_body.geography is None


def test_api_geography_create_patch(ctx_public_namespace_read_write):
    ctx = ctx_public_namespace_read_write
    namespace = ctx.namespace.path

    unit_box = box(0, 0, 1, 1)
    box_wkb = shapely.wkb.dumps(unit_box)
    create_response = ctx.client.post(
        f"{GEOS_ROOT}/{namespace}",
        headers=headers(ctx),
        content=msgpack.dumps([{"path": "box", "geography": box_wkb}]),
    )
    assert create_response.status_code == HTTPStatus.CREATED, msgpack.loads(
        create_response.content
    )
    create_body = schemas.Geography(**msgpack.loads(create_response.content)[0])

    # Implicitly create a new GeoVersion for the geography.
    shifted_unit_box = box(1, 1, 2, 2)
    shifted_internal_point = Point(1.5, 1.5)
    patch_response = ctx.client.patch(
        f"{GEOS_ROOT}/{namespace}",
        headers=headers(ctx),
        content=msgpack.dumps(
            [
                {
                    "path": "box",
                    "geography": shapely.wkb.dumps(shifted_unit_box),
                    "internal_point": shapely.wkb.dumps(shifted_internal_point),
                }
            ]
        ),
    )
    assert patch_response.status_code == HTTPStatus.OK, msgpack.loads(
        patch_response.content
    )

    patch_body = schemas.Geography(**msgpack.loads(patch_response.content)[0])
    assert shapely.wkb.loads(patch_body.geography) == shifted_unit_box
    assert shapely.wkb.loads(patch_body.internal_point) == shifted_internal_point
    assert patch_body.valid_from > create_body.valid_from


def test_api_geography_patch__nonexistent(ctx_public_namespace_read_write):
    ctx = ctx_public_namespace_read_write
    namespace = ctx.namespace.path

    unit_box = box(0, 0, 1, 1)
    box_wkb = shapely.wkb.dumps(unit_box)
    patch_response = ctx.client.patch(
        f"{GEOS_ROOT}/{namespace}",
        headers=headers(ctx),
        content=msgpack.dumps([{"path": "box", "geography": box_wkb}]),
    )
    assert (
        patch_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    ), patch_response.json()
