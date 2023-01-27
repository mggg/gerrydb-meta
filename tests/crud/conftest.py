"""Tests for CherryDB CRUD operations."""
import pytest

from cherrydb_meta import models


@pytest.fixture
def db_with_user(db):
    """SQLAlchemy ORM session with a fake `User` model."""
    user = models.User(email="test@example.com", name="Test User")
    db.add(user)
    db.flush()
    yield db, user


@pytest.fixture
def db_with_meta(db_with_user):
    db, user = db_with_user
    meta = models.ObjectMeta(notes="test", created_by=user.user_id)
    db.add(meta)
    db.flush()
    yield db, meta
