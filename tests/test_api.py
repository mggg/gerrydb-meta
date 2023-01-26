"""Tests for CherryDB REST API."""
import pytest
from http import HTTPStatus
from typing import Generator
from fastapi.testclient import TestClient
from cherrydb_meta import models, schemas
from cherrydb_meta.admin import CherryAdmin
from cherrydb_meta.api.deps import get_db
from cherrydb_meta.main import app, API_PREFIX

META_ROOT = f"{API_PREFIX}/meta"
LOCALITIES_ROOT = f"{API_PREFIX}/localities"
    
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
   

def test_api_object_meta_create_read(client_with_user):
    client, user = client_with_user
    notes = "test"
   
    # Create new metadata. 
    create_response = client.post(f"{META_ROOT}/", json={"notes": notes})
    assert create_response.status_code == HTTPStatus.CREATED
    create_body = schemas.ObjectMeta(**create_response.json())
    assert create_body.notes == notes
    assert create_body.created_by == user.email
   
    # Read it back. 
    read_response = client.get(f"{META_ROOT}/{create_body.meta_id}")
    assert read_response.status_code == HTTPStatus.OK
    read_body = schemas.ObjectMeta(**read_response.json())
    assert read_body == create_body
   

def test_api_locality_create_read_no_parent_no_aliases(client_with_meta):
    client, meta = client_with_meta
    name = "Lost City of Atlantis"
    path = "atlantis"
     
    # Create new locality. 
    create_response = client.post(
        f"{LOCALITIES_ROOT}/",
        json={"name":  name, "canonical_path": path}
    )
    assert create_response.status_code == HTTPStatus.CREATED
    create_body = schemas.Locality(**create_response.json())
    assert create_body.name == name
    assert create_body.canonical_path == path
    assert create_body.parent_path is None
    assert create_body.meta.meta_id == meta.meta_id
   
    # Read it back. 
    read_response = client.get(f"{API_PREFIX}/localities/{path}")
    assert read_response.status_code == HTTPStatus.OK
    read_body = schemas.Locality(**read_response.json())
    assert read_body == create_body
    
    
def test_api_locality_create_read_parent_and_aliases(client_with_meta):
    client, meta = client_with_meta
    name = "Lost City of Atlantis"
    path = "greece/atlantis"
    parent_path = "greece"
    aliases = ["atlantis", "g/atlantis"]
     
    # Create parent locality.
    create_parent_response = client.post(
        f"{LOCALITIES_ROOT}/",
        json={
            "name":  "Greece",
            "canonical_path": parent_path
        }
    )
    assert create_parent_response.status_code == HTTPStatus.CREATED
    
    # Create child locality with aliases.
    create_child_response = client.post(
        f"{LOCALITIES_ROOT}/",
        json={
            "name": name,
            "canonical_path": path,
            "parent_path": parent_path,
            "aliases": aliases
        }
    )
    assert create_child_response.status_code == HTTPStatus.CREATED
    create_child_body = schemas.Locality(**create_child_response.json())
    assert create_child_body.name == name
    assert create_child_body.canonical_path == path
    assert create_child_body.aliases == aliases
    assert create_child_body.parent_path == parent_path
    assert create_child_body.meta.meta_id == meta.meta_id
    
    # Read back the parent and the child.
    read_response = client.get(f"{LOCALITIES_ROOT}/")
    assert read_response.status_code == HTTPStatus.OK
    read_body = [schemas.Locality(**obj) for obj in read_response.json()]
    assert set(loc.canonical_path for loc in read_body) == {path, parent_path}