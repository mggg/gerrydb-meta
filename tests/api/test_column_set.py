"""Tests for GerryDB REST API column set endpoints."""
from http import HTTPStatus

import pytest

from gerrydb_meta import crud, models, schemas
from gerrydb_meta.enums import ColumnKind, ColumnType, ScopeType
from gerrydb_meta.main import API_PREFIX

from .scopes import revoke_scope_type

COLUMN_SETS_ROOT = f"{API_PREFIX}/column-sets"


def create_column(
    ctx, path: str = "test", aliases: list[str] | None = None
) -> models.Namespace:
    """Creates a column in a test context's namespace (direct CRUD)."""
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


def test_api_column_set_create_read(namespaced_read_write_ctx):
    ctx = namespaced_read_write_ctx
    namespace = ctx.namespace.path

    create_column(ctx, "test1")
    create_column(ctx, "test2")

    create_response = ctx.client.post(
        f"{COLUMN_SETS_ROOT}/{namespace}",
        json={
            "path": "cols",
            "description": "A basic column set.",
            "columns": ["test1", "test2"],
        },
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()
    create_body = schemas.ColumnSet(**create_response.json())

    read_response = ctx.client.get(
        f"{COLUMN_SETS_ROOT}/{namespace}/cols",
    )
    assert read_response.status_code == HTTPStatus.OK, read_response.json()
    read_body = schemas.ColumnSet(**read_response.json())
    assert read_body == create_body


def test_api_column_set_create_all(namespaced_read_write_ctx):
    ctx = namespaced_read_write_ctx
    namespace = ctx.namespace.path

    create_column(ctx, "test")
    create_response = ctx.client.post(
        f"{COLUMN_SETS_ROOT}/{namespace}",
        json={
            "path": "cols",
            "description": "A basic column set.",
            "columns": ["test"],
        },
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()
    create_body = schemas.ColumnSet(**create_response.json())

    all_response = ctx.client.get(
        f"{COLUMN_SETS_ROOT}/{namespace}",
    )
    assert all_response.status_code == HTTPStatus.OK, all_response.json()
    read_body = schemas.ColumnSet(**all_response.json()[0])
    assert read_body == create_body


def test_api_column_set_create_read__scope_read_only(namespaced_read_only_ctx):
    ctx = namespaced_read_only_ctx
    namespace = ctx.namespace.path

    create_response = ctx.client.post(
        f"{COLUMN_SETS_ROOT}/{namespace}",
        json={
            "path": "cols",
            "description": "A basic column set.",
            "columns": [],
        },
    )
    assert create_response.status_code == HTTPStatus.NOT_FOUND, create_response.json()


def test_api_column_set_create__column_aliases(namespaced_read_write_ctx):
    ctx = namespaced_read_write_ctx
    namespace = ctx.namespace.path

    create_column(ctx, "test1", aliases=["t1", "x"])
    create_column(ctx, "test2", aliases=["t2", "y"])

    create_response = ctx.client.post(
        f"{COLUMN_SETS_ROOT}/{namespace}",
        json={
            "path": "cols",
            "description": "A basic column set.",
            "columns": ["y", "x"],
        },
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()
    create_body = schemas.ColumnSet(**create_response.json())
    assert create_body.refs == ["y", "x"]


def test_api_column_set_create__duplicate_column(namespaced_read_write_ctx):
    ctx = namespaced_read_write_ctx
    namespace = ctx.namespace.path

    create_column(ctx, "test")

    create_response = ctx.client.post(
        f"{COLUMN_SETS_ROOT}/{namespace}",
        json={
            "path": "cols",
            "description": "A column set with a duplicate column.",
            "columns": ["test", "test"],
        },
    )
    assert (
        create_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    ), create_response.json()


def test_api_column_set_create__aliased_duplicate_column(namespaced_read_write_ctx):
    ctx = namespaced_read_write_ctx
    namespace = ctx.namespace.path

    create_column(ctx, "test", aliases=["t"])

    create_response = ctx.client.post(
        f"{COLUMN_SETS_ROOT}/{namespace}",
        json={
            "path": "cols",
            "description": "A column set with a duplicate column.",
            "columns": ["t", "test"],
        },
    )
    assert (
        create_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    ), create_response.json()


def test_api_column_set_create__missing_column(namespaced_read_write_ctx):
    ctx = namespaced_read_write_ctx
    namespace = ctx.namespace.path

    create_response = ctx.client.post(
        f"{COLUMN_SETS_ROOT}/{namespace}",
        json={
            "path": "cols",
            "description": "A column set that references a nonexistent column.",
            "columns": ["bad"],
        },
    )
    assert (
        create_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    ), create_response.json()


def test_api_column_set_create_read__private_namespace(
    private_namespace_read_write_ctx,
):
    ctx = private_namespace_read_write_ctx
    namespace = ctx.namespace.path
    create_response = ctx.client.post(
        f"{COLUMN_SETS_ROOT}/{namespace}",
        json={"path": "cols", "description": "empty", "columns": []},
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    # Revoke access to the private namespace.
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_READ)
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_WRITE)

    read_response = ctx.client.get(f"{COLUMN_SETS_ROOT}/{namespace}/cols")
    assert read_response.status_code == HTTPStatus.NOT_FOUND, read_response.json()


def test_api_column_create_all__private_namespace(private_namespace_read_write_ctx):
    ctx = private_namespace_read_write_ctx
    namespace = ctx.namespace.path
    create_response = ctx.client.post(
        f"{COLUMN_SETS_ROOT}/{namespace}",
        json={"path": "cols", "description": "empty", "columns": []},
    )
    assert create_response.status_code == HTTPStatus.CREATED, create_response.json()

    # Revoke access to the private namespace.
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_READ)
    revoke_scope_type(ctx.db, ctx.meta, ScopeType.NAMESPACE_WRITE)

    all_response = ctx.client.get(f"{COLUMN_SETS_ROOT}/{namespace}")
    assert all_response.status_code == HTTPStatus.NOT_FOUND, all_response.json()
