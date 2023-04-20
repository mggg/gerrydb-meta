"""Fixtures for REST API tests."""
from dataclasses import dataclass
from typing import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from gerrydb_meta import crud, models, schemas
from gerrydb_meta.admin import GerryAdmin
from gerrydb_meta.api.deps import get_db
from gerrydb_meta.enums import NamespaceGroup, ScopeType
from gerrydb_meta.main import app

from .scopes import grant_namespaced_scope, grant_scope


@pytest.fixture
def client_no_auth(db):
    """FastAPI test client with no authentication."""

    def get_test_db() -> Generator:
        yield db

    app.dependency_overrides[get_db] = get_test_db
    yield TestClient(app)


@pytest.fixture
def db_and_client_with_user_no_scopes(db):
    """Database session and FastAPI test client with API key authentication."""

    def get_test_db() -> Generator:
        yield db

    admin = GerryAdmin(db)
    user = admin.user_create(name="Test User", email="test@example.com")
    api_key = admin.key_create(user)
    db.flush()
    db.refresh(user)

    app.dependency_overrides[get_db] = get_test_db
    client = TestClient(app)
    client.headers = {"X-API-Key": api_key}
    yield db, client, user


@pytest.fixture
def client_with_user_no_scopes(db_and_client_with_user_no_scopes):
    """FastAPI test client with API key authentication."""
    _, client, user = db_and_client_with_user_no_scopes
    yield client, user


@pytest.fixture
def client_with_superuser(db_and_client_with_user_no_scopes):
    """FastAPI test client with API key authentication and maximum privileges."""
    db, client, user = db_and_client_with_user_no_scopes
    grant_scope(db, user, ScopeType.ALL, NamespaceGroup.ALL)
    yield client, user


@pytest.fixture
def db_and_client_with_meta_no_scopes(db):
    """Database session + test client with API key auth and metadata context."""

    def get_test_db() -> Generator:
        yield db

    # TODO: replace with `crud` calls.
    admin = GerryAdmin(db)
    user = admin.user_create(name="Test User", email="test@example.com")
    api_key = admin.key_create(user)
    db.flush()
    meta = models.ObjectMeta(notes="metameta", created_by=user.user_id)
    db.add(meta)
    db.flush()
    db.refresh(user)
    db.refresh(meta)

    app.dependency_overrides[get_db] = get_test_db
    client = TestClient(app)
    client.headers = {"X-API-Key": api_key, "X-GerryDB-Meta-Id": str(meta.uuid)}
    yield db, client, meta


@pytest.fixture
def client_with_meta_no_scopes(db_and_client_with_meta_no_scopes):
    """Test client with API key auth and metadata context."""
    _, client, meta = db_and_client_with_meta_no_scopes
    yield client, meta


@pytest.fixture
def client_with_meta_superuser(db_and_client_with_meta_no_scopes):
    """Test client with API key auth and metadata context."""
    db, client, meta = db_and_client_with_meta_no_scopes
    grant_scope(db, meta, ScopeType.ALL, NamespaceGroup.ALL)
    yield client, meta


@pytest.fixture
def client_with_meta_locality(db_and_client_with_meta_locality):
    """An API client with `LOCALITY_READ` and `LOCALITY_WRITE` scopes (+ session and metadata)."""
    _, client, meta = db_and_client_with_meta_locality
    yield client, meta


# Extend this for all new tests rather than returning tuples from fixtures.
@dataclass(frozen=True)
class TestContext:
    """Database context for an API test."""

    db: Session
    client: TestClient
    meta: models.ObjectMeta | None = None
    namespace: models.Namespace | None = None


@pytest.fixture
def namespaced_read_only_ctx(request, db_and_client_with_meta_no_scopes):
    """Context with an API client with NAMESPACE_READ scope in a namespace."""
    db, client, meta = db_and_client_with_meta_no_scopes
    test_name = request.node.name.replace("[", "__").replace("]", "")
    namespace, _ = crud.namespace.create(
        db=db,
        obj_in=schemas.NamespaceCreate(
            path=test_name,
            description=f"Namespace for test {request.node.name}",
            public=True,
        ),
        obj_meta=meta,
    )
    grant_namespaced_scope(db, meta, namespace, ScopeType.NAMESPACE_READ)
    yield TestContext(db=db, client=client, meta=meta, namespace=namespace)


@pytest.fixture
def namespaced_read_write_ctx(namespaced_read_only_ctx):
    """Context with an API client with NAMESPACE_READ scope in a namespace."""
    ctx = namespaced_read_only_ctx
    grant_namespaced_scope(ctx.db, ctx.meta, ctx.namespace, ScopeType.NAMESPACE_WRITE)
    yield ctx


@pytest.fixture
def private_namespace_read_only_ctx(request, db_and_client_with_meta_no_scopes):
    """Context with an API client with NAMESPACE_READ scope in a private namespace."""
    db, client, meta = db_and_client_with_meta_no_scopes
    test_name = request.node.name.replace("[", "__").replace("]", "")
    namespace, _ = crud.namespace.create(
        db=db,
        obj_in=schemas.NamespaceCreate(
            path=test_name,
            description=f"Private namespace for test {request.node.name}",
            public=False,
        ),
        obj_meta=meta,
    )
    grant_namespaced_scope(db, meta, namespace, ScopeType.NAMESPACE_READ)
    yield TestContext(db=db, client=client, meta=meta, namespace=namespace)


@pytest.fixture
def private_namespace_read_write_ctx(private_namespace_read_only_ctx):
    """Context with an API client with NAMESPACE_READ scope in a private namespace."""
    ctx = private_namespace_read_only_ctx
    grant_namespaced_scope(ctx.db, ctx.meta, ctx.namespace, ScopeType.NAMESPACE_WRITE)
    yield ctx


@pytest.fixture
def pop_column_meta():
    """Example metadata for a population column."""
    return {
        "canonical_path": "total_pop",
        "description": "2020 Census total population",
        "source_url": "https://www.census.gov/",
        "kind": "count",
        "type": "int",
        "aliases": ["totpop", "p001001", "p0001001"],
    }


@pytest.fixture
def vap_column_meta():
    """Example metadata for a voting-age population column."""
    return {
        "canonical_path": "total_vap",
        "description": "2020 Census total voting-age population (VAP)",
        "source_url": "https://www.census.gov/",
        "kind": "count",
        "type": "float",
        "aliases": ["totvap", "p003001", "p0003001"],
    }
