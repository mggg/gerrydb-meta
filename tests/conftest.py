"""Test configuration for CherryDB."""
import os
import geoalchemy2
import pytest
from sqlalchemy import create_engine, schema
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker
from cherrydb_meta import models

DEFAULT_TEST_DATABASE_URI = "postgresql://postgres:test@localhost:54321"

@pytest.fixture(scope="session")
def db_engine():
    """SpatialLite-enabled SQLAlchemy engine."""
    engine = create_engine(os.getenv("CHERRY_TEST_DATABASE_URI", DEFAULT_TEST_DATABASE_URI))
    yield engine
    engine.dispose()
    

@pytest.fixture
def db(db_engine):
    """SQLAlchemy ORM session with CherryDB tables initialized.""" 
    with db_engine.connect() as conn:
        conn.execute("DROP SCHEMA IF EXISTS cherrydb CASCADE")
        conn.execute("CREATE SCHEMA cherrydb")
        models.Base.metadata.create_all(db_engine)
        session = sessionmaker(db_engine)()
        yield session
        session.close()
        conn.execute("DROP SCHEMA cherrydb CASCADE")


@pytest.fixture
def db_with_user(db):
    """SQLAlchemy ORM session with a fake `User` model."""
    user = models.User(email="test@example.com", name="Test User")
    db.add(user) 
    db.commit()
    yield db, user


@pytest.fixture
def db_with_meta(db_with_user):
    db, user = db_with_user
    meta = models.ObjectMeta(notes="test", created_by=user.user_id)
    db.add(meta)
    db.commit()
    yield db, meta
