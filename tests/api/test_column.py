"""Tests for GerryDB REST API column endpoints."""
from http import HTTPStatus

from gerrydb_meta import schemas
from gerrydb_meta.enums import ColumnKind, ColumnType, ScopeType
from gerrydb_meta.main import API_PREFIX

from .scopes import revoke_scope_type

COLUMNS_ROOT = f"{API_PREFIX}/columns"


def test_api_column_create_read(namespaced_read_write_ctx, pop_column_meta):
    namespace = namespaced_read_write_ctx.namespace.path
    create_response = namespaced_read_write_ctx.client.post(
        f"{COLUMNS_ROOT}/{namespace}", json=pop_column_meta
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    create_body = schemas.Column(**create_response.json())
    assert create_body.canonical_path == pop_column_meta["canonical_path"]
    assert create_body.description == pop_column_meta["description"]
    assert create_body.source_url == pop_column_meta["source_url"]
    assert create_body.kind == ColumnKind.COUNT
    assert create_body.type == ColumnType.INT
    assert set(create_body.aliases) == set(pop_column_meta["aliases"])

    read_response = namespaced_read_write_ctx.client.get(
        f"{COLUMNS_ROOT}/{namespace}/{create_body.canonical_path}"
    )
    assert read_response.status_code == HTTPStatus.OK, read_response.json()
    read_body = schemas.Column(**read_response.json())
    assert read_body == create_body


def test_api_column_create_read__get_by_alias(
    namespaced_read_write_ctx, pop_column_meta
):
    namespace = namespaced_read_write_ctx.namespace.path
    create_response = namespaced_read_write_ctx.client.post(
        f"{COLUMNS_ROOT}/{namespace}", json=pop_column_meta
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()
    create_body = schemas.Column(**create_response.json())

    alias = pop_column_meta["aliases"][0]
    read_response = namespaced_read_write_ctx.client.get(
        f"{COLUMNS_ROOT}/{namespace}/{alias}"
    )
    assert read_response.status_code == HTTPStatus.OK, read_response.json()
    read_body = schemas.Column(**read_response.json())
    assert read_body == create_body


def test_api_column_create_patch(namespaced_read_write_ctx, pop_column_meta):
    namespace = namespaced_read_write_ctx.namespace.path
    create_response = namespaced_read_write_ctx.client.post(
        f"{COLUMNS_ROOT}/{namespace}", json=pop_column_meta
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    path = pop_column_meta["canonical_path"]
    patch_response = namespaced_read_write_ctx.client.patch(
        f"{COLUMNS_ROOT}/{namespace}/{path}",
        json={"aliases": ["another_alias"]},
    )
    assert patch_response.status_code == HTTPStatus.OK, patch_response.json()
    patch_body = schemas.Column(**patch_response.json())
    assert set(patch_body.aliases) == {*pop_column_meta["aliases"], "another_alias"}


def test_api_column_create_all(
    namespaced_read_write_ctx, pop_column_meta, vap_column_meta
):
    namespace = namespaced_read_write_ctx.namespace.path
    canonical_paths = set()
    for col_meta in (pop_column_meta, vap_column_meta):
        create_response = namespaced_read_write_ctx.client.post(
            f"{COLUMNS_ROOT}/{namespace}", json=col_meta
        )
        canonical_paths.add(col_meta["canonical_path"])
        assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    all_response = namespaced_read_write_ctx.client.get(f"{COLUMNS_ROOT}/{namespace}")
    assert all_response.status_code == HTTPStatus.OK, all_response.json()
    assert set(col["canonical_path"] for col in all_response.json()) == canonical_paths


def test_api_column_create_read__scope_read_only(
    namespaced_read_only_ctx, pop_column_meta
):
    namespace = namespaced_read_only_ctx.namespace.path
    create_response = namespaced_read_only_ctx.client.post(
        f"{COLUMNS_ROOT}/{namespace}", json=pop_column_meta
    )
    assert create_response.status_code == HTTPStatus.NOT_FOUND, create_response.json()

    read_response = namespaced_read_only_ctx.client.get(
        f"{COLUMNS_ROOT}/{namespace}/{pop_column_meta['canonical_path']}"
    )
    assert read_response.status_code == HTTPStatus.NOT_FOUND, read_response.json()


def test_api_column_create_read__private_namespace(
    private_namespace_read_write_ctx, pop_column_meta
):
    ctx = private_namespace_read_write_ctx
    namespace = ctx.namespace.path
    create_response = ctx.client.post(
        f"{COLUMNS_ROOT}/{namespace}", json=pop_column_meta
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    # Revoke access to the private namespace.
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_READ)
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_WRITE)

    read_response = ctx.client.get(
        f"{COLUMNS_ROOT}/{namespace}/{pop_column_meta['canonical_path']}"
    )
    assert read_response.status_code == HTTPStatus.NOT_FOUND, read_response.json()


def test_api_column_create_all__private_namespace(
    private_namespace_read_write_ctx, pop_column_meta
):
    ctx = private_namespace_read_write_ctx
    namespace = ctx.namespace.path
    create_response = ctx.client.post(
        f"{COLUMNS_ROOT}/{namespace}", json=pop_column_meta
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    # Revoke access to the private namespace.
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_READ)
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_WRITE)

    read_response = ctx.client.get(f"{COLUMNS_ROOT}/{namespace}")
    assert read_response.status_code == HTTPStatus.NOT_FOUND, read_response.json()


def test_api_column_create_patch__private_namespace(
    private_namespace_read_write_ctx, pop_column_meta
):
    ctx = private_namespace_read_write_ctx
    namespace = ctx.namespace.path
    create_response = ctx.client.post(
        f"{COLUMNS_ROOT}/{namespace}", json=pop_column_meta
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    # Revoke access to the private namespace.
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_READ)
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_WRITE)

    patch_response = ctx.client.patch(
        f"{COLUMNS_ROOT}/{namespace}/{pop_column_meta['canonical_path']}",
        json={"aliases": ["another_alias"]},
    )
    assert patch_response.status_code == HTTPStatus.NOT_FOUND, patch_response.json()
