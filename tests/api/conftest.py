"""Fixtures for REST API tests."""
import pytest
from typing import Generator
from fastapi.testclient import TestClient
from cherrydb_meta import models
from cherrydb_meta.admin import CherryAdmin
from cherrydb_meta.api.deps import get_db
from cherrydb_meta.main import app


@pytest.fixture
def client_no_auth(db):
    """FastAPI test client with no authentication.""" 
   
    def get_test_db() -> Generator:
        yield db
        
    app.dependency_overrides[get_db] = get_test_db
    yield TestClient(app)
    
    
@pytest.fixture
def client_with_user(db):
    """FastAPI test client with API key authentication.""" 
   
    def get_test_db() -> Generator:
        yield db
        
    admin = CherryAdmin(db) 
    user = admin.user_create(name="Test User", email="test@example.com")
    api_key = admin.key_create(user)
    
    app.dependency_overrides[get_db] = get_test_db
    client = TestClient(app)
    client.headers = {"X-API-Key": api_key}
    yield client, user
    
    
@pytest.fixture
def client_with_meta(db):
    """FastAPI test client with API key authentication and metadata context.""" 
   
    def get_test_db() -> Generator:
        yield db
        
    admin = CherryAdmin(db) 
    user = admin.user_create(name="Test User", email="test@example.com")
    api_key = admin.key_create(user)
    db.flush()
    meta = models.ObjectMeta(notes="metameta", created_by=user.user_id)
    db.add(meta)
    db.flush()
    
    app.dependency_overrides[get_db] = get_test_db
    client = TestClient(app)
    client.headers = {"X-API-Key": api_key, "X-Cherry-Meta-Id": str(meta.meta_id)}
    yield client, meta