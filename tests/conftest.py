"""Test configuration for CherryDB."""
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker

from cherrydb_meta import models

DEFAULT_TEST_DATABASE_URI = "postgresql://postgres:test@localhost:54321"


@pytest.fixture(scope="session")
def db_engine():
    """SpatialLite-enabled SQLAlchemy engine."""
    engine = create_engine(
        os.getenv("CHERRY_TEST_DATABASE_URI", DEFAULT_TEST_DATABASE_URI)
    )
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def db_schema(db_engine):
    """SQLAlchemy ORM session maker with CherryDB schema initialized."""
    with db_engine.connect() as conn:
        init_transaction = conn.begin()
        conn.execute(text("DROP SCHEMA IF EXISTS cherrydb_test"))
        conn.execute(text("CREATE SCHEMA cherrydb"))
        init_transaction.commit()

        models.Base.metadata.create_all(db_engine)
        yield sessionmaker(db_engine)

        cleanup_transaction = conn.begin()
        conn.execute(text("DROP SCHEMA cherrydb CASCADE"))
        cleanup_transaction.commit()


@pytest.fixture
def db(db_schema):
    """SQLAlchemy ORM session (rolls back on cleanup)."""
    session = db_schema()
    yield session
    session.rollback()
    session.close()
