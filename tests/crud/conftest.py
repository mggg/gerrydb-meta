"""Tests for GerryDB CRUD operations."""

import pytest

from gerrydb_meta import models


@pytest.fixture
def db_with_user(db):
    """SQLAlchemy ORM session with a fake `User` model."""
    user = models.User(email="test@example.com", name="Test User")
    db.add(user)
    db.flush()
    yield db, user


@pytest.fixture
def db_with_user_api_key(db_with_user):
    db, user = db_with_user
    api_key = models.ApiKey(key_hash=b"somehash", user_id=user.user_id, user=user)
    db.add(api_key)
    db.flush()
    yield db, api_key


@pytest.fixture
def db_with_meta(db_with_user):
    db, user = db_with_user
    meta = models.ObjectMeta(notes="test", created_by=user.user_id)
    db.add(meta)
    db.flush()
    yield db, meta


@pytest.fixture
def db_with_meta_and_user(db_with_user):
    db, user = db_with_user
    meta = models.ObjectMeta(notes="test", created_by=user.user_id)
    db.add(meta)
    db.flush()
    yield db, meta, user
