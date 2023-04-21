"""Tests for GerryDB REST API GeoImport endpoints."""
from http import HTTPStatus

from gerrydb_meta import schemas
from gerrydb_meta.main import API_PREFIX

GEO_IMPORTS_ROOT = f"{API_PREFIX}/geo-imports"


def test_api_geo_import_create_read(ctx_public_namespace_read_write):
    ctx = ctx_public_namespace_read_write
    namespace = ctx.namespace.path

    create_response = ctx.client.post(f"{GEO_IMPORTS_ROOT}/{namespace}")
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()
    create_body = schemas.GeoImport(**create_response.json())

    read_response = ctx.client.get(f"{GEO_IMPORTS_ROOT}/{namespace}/{create_body.uuid}")
    assert read_response.status_code == HTTPStatus.OK, read_response.json()
    read_body = schemas.GeoImport(**read_response.json())

    assert create_body == read_body


def test_api_geo_import_create_all(ctx_public_namespace_read_write):
    ctx = ctx_public_namespace_read_write
    namespace = ctx.namespace.path

    create_response = ctx.client.post(f"{GEO_IMPORTS_ROOT}/{namespace}")
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    all_response = ctx.client.get(f"{GEO_IMPORTS_ROOT}/{namespace}")
    assert all_response.status_code == HTTPStatus.OK, all_response.json()
    assert len(all_response.json()) == 1

    create_body = schemas.GeoImport(**create_response.json())
    all_body = schemas.GeoImport(**all_response.json()[0])
    assert create_body == all_body


def test_api_geo_import_create__scope_read_only(ctx_public_namespace_read_only):
    ctx = ctx_public_namespace_read_only
    namespace = ctx.namespace.path

    create_response = ctx.client.post(f"{GEO_IMPORTS_ROOT}/{namespace}")
    assert create_response.status_code == HTTPStatus.NOT_FOUND, create_response.json()


def test_api_geo_import_create_read__private_namespace(
    ctx_public_namespace_read_write, ctx_private_namespace_read_write
):
    public_ctx = ctx_public_namespace_read_write
    private_ctx = ctx_private_namespace_read_write
    namespace = private_ctx.namespace.path

    create_response = private_ctx.client.post(f"{GEO_IMPORTS_ROOT}/{namespace}")
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()
    create_body = schemas.GeoImport(**create_response.json())

    read_response = public_ctx.client.get(
        f"{GEO_IMPORTS_ROOT}/{namespace}/{create_body.uuid}"
    )
    assert read_response.status_code == HTTPStatus.NOT_FOUND, read_response.json()


def test_api_geo_import_create_all__private_namespace(
    ctx_public_namespace_read_write, ctx_private_namespace_read_write
):
    public_ctx = ctx_public_namespace_read_write
    private_ctx = ctx_private_namespace_read_write
    namespace = private_ctx.namespace.path

    create_response = private_ctx.client.post(f"{GEO_IMPORTS_ROOT}/{namespace}")
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    all_response = public_ctx.client.get(f"{GEO_IMPORTS_ROOT}/{namespace}")
    assert all_response.status_code == HTTPStatus.NOT_FOUND, all_response.json()
