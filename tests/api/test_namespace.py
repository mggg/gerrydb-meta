"""Tests for GerryDB REST API namespace endpoints."""
from http import HTTPStatus

import pytest
from sqlalchemy.orm import Session

from gerrydb_meta import crud, models, schemas
from gerrydb_meta.enums import NamespaceGroup, ScopeType
from gerrydb_meta.main import API_PREFIX

from .scopes import grant_scope

NAMESPACES_ROOT = f"{API_PREFIX}/namespaces"


@pytest.fixture
def client_with_namespace_rc_all(db_and_client_with_meta_no_scopes):
    """Client with global read/create namespace scopes."""
    db, client, meta = db_and_client_with_meta_no_scopes
    grant_scope(db, meta, ScopeType.NAMESPACE_READ, namespace_group=NamespaceGroup.ALL)
    grant_scope(db, meta, ScopeType.NAMESPACE_CREATE)
    yield client


@pytest.fixture
def client_with_namespace_ro_all(db_and_client_with_meta_no_scopes):
    """Client with global read/write namespace scopes."""
    db, client, meta = db_and_client_with_meta_no_scopes
    grant_scope(db, meta, ScopeType.NAMESPACE_READ, namespace_group=NamespaceGroup.ALL)
    yield client


@pytest.fixture
def db_and_client_with_namespace_rc_public(db_and_client_with_meta_no_scopes):
    """Client with public read/write/create namespace scopes (+ session and metadata)."""
    db, client, meta = db_and_client_with_meta_no_scopes
    grant_scope(
        db, meta, ScopeType.NAMESPACE_READ, namespace_group=NamespaceGroup.PUBLIC
    )
    grant_scope(db, meta, ScopeType.NAMESPACE_CREATE)
    yield db, client, meta


@pytest.fixture
def client_with_namespace_rc_public(db_and_client_with_namespace_rc_public):
    """Client with public read/write/create namespace scopes."""
    _, client, _ = db_and_client_with_namespace_rc_public
    yield client


def test_api_namespace_create_read__public(client_with_namespace_rc_all):
    # Create new namespace.
    path = "census.2020"
    description = "2020 Census data"

    create_response = client_with_namespace_rc_all.post(
        f"{NAMESPACES_ROOT}/",
        json={"path": path, "description": description, "public": True},
    )
    assert create_response.status_code == HTTPStatus.CREATED
    create_body = schemas.Namespace(**create_response.json())
    assert create_body.path == path
    assert create_body.description == description
    assert create_body.public

    # Read it back.
    read_response = client_with_namespace_rc_all.get(f"{NAMESPACES_ROOT}/{path}")
    assert read_response.status_code == HTTPStatus.OK
    read_body = schemas.Namespace(**read_response.json())
    assert read_body == create_body


def test_api_namespace_create__twice(client_with_namespace_rc_all):
    body = {"path": "census", "description": "Census data", "public": True}
    create_response = client_with_namespace_rc_all.post(
        f"{NAMESPACES_ROOT}/", json=body
    )
    assert create_response.status_code == HTTPStatus.CREATED

    create_again_response = client_with_namespace_rc_all.post(
        f"{NAMESPACES_ROOT}/", json=body
    )
    assert create_again_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


def test_api_namespace_create_ro(client_with_namespace_ro_all):
    # Create new namespace.
    create_response = client_with_namespace_ro_all.post(
        f"{NAMESPACES_ROOT}/",
        json={"path": "census", "description": "Census data", "public": True},
    )
    assert create_response.status_code == HTTPStatus.FORBIDDEN
    assert "permissions to create" in create_response.json()["detail"]


def test_api_namespace_read__missing(client_with_namespace_rc_all):
    read_response = client_with_namespace_rc_all.get(f"{NAMESPACES_ROOT}/missing")
    assert read_response.status_code == HTTPStatus.NOT_FOUND


def test_api_namespace_create_read__private(db_and_client_with_namespace_rc_public):
    db, client, meta = db_and_client_with_namespace_rc_public
    crud.namespace.create(
        db=db,
        obj_in=schemas.NamespaceCreate(
            path="private", description="secret!", public=False
        ),
        obj_meta=meta,
    )
    read_response = client.get(f"{NAMESPACES_ROOT}/private")
    assert read_response.status_code == HTTPStatus.NOT_FOUND


def test_api_namespace_all__private(db_and_client_with_namespace_rc_public):
    db, client, meta = db_and_client_with_namespace_rc_public
    crud.namespace.create(
        db=db,
        obj_in=schemas.NamespaceCreate(
            path="private", description="secret!", public=False
        ),
        obj_meta=meta,
    )
    crud.namespace.create(
        db=db,
        obj_in=schemas.NamespaceCreate(
            path="public", description="not secret!", public=True
        ),
        obj_meta=meta,
    )

    list_response = client.get(f"{NAMESPACES_ROOT}/")
    assert list_response.status_code == HTTPStatus.OK
    list_body = list_response.json()
    assert len(list_body) == 1
    assert schemas.Namespace(**list_body[0]).path == "public"
