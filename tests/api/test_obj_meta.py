
"""Tests for CherryDB REST API object metadata endpoints."""
from http import HTTPStatus
from cherrydb_meta import schemas
from cherrydb_meta.main import API_PREFIX

META_ROOT = f"{API_PREFIX}/meta"

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
   