"""Fixtures for REST API tests."""
from typing import Generator

import pytest
from fastapi.testclient import TestClient

from cherrydb_meta import models
from cherrydb_meta.admin import CherryAdmin
from cherrydb_meta.api.deps import get_db
from cherrydb_meta.main import app
from cherrydb_meta.enums import ScopeType, NamespaceGroup
from .scopes import grant_scope


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

    admin = CherryAdmin(db)
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
    admin = CherryAdmin(db)
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
    client.headers = {"X-API-Key": api_key, "X-Cherry-Meta-Id": str(meta.uuid)}
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
