"""Test configuration for GerryDB."""
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker

from gerrydb_meta import models

DEFAULT_TEST_DATABASE_URI = "postgresql://postgres:test@localhost:54321"


@pytest.fixture(scope="session")
def db_engine():
    """SpatialLite-enabled SQLAlchemy engine."""
    engine = create_engine(
        os.getenv("GERRYDB_TEST_DATABASE_URI", DEFAULT_TEST_DATABASE_URI)
    )
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def db_schema(db_engine):
    """SQLAlchemy ORM session maker with GerryDB schema initialized."""
    with db_engine.connect() as conn:
        init_transaction = conn.begin()
        conn.execute(text("DROP SCHEMA IF EXISTS gerrydb_test"))
        conn.execute(text("CREATE SCHEMA gerrydb"))
        init_transaction.commit()

        models.Base.metadata.create_all(db_engine)
        yield sessionmaker(db_engine)

        cleanup_transaction = conn.begin()
        conn.execute(text("DROP SCHEMA gerrydb CASCADE"))
        cleanup_transaction.commit()


@pytest.fixture
def db(db_schema):
    """SQLAlchemy ORM session (rolls back on cleanup)."""
    session = db_schema()
    yield session
    session.rollback()
    session.close()
