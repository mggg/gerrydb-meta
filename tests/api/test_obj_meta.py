"""Tests for CherryDB REST API object metadata endpoints."""
from http import HTTPStatus

from sqlalchemy.orm import Session
from cherrydb_meta import crud, models, schemas
from cherrydb_meta.enums import ScopeType
from cherrydb_meta.main import API_PREFIX
from .scopes import grant_scope

META_ROOT = f"{API_PREFIX}/meta"


def create_new_user_meta(db: Session) -> models.ObjectMeta:
    """Creates metadata associated with a new user."""

    # TODO: fix this abstraction violation (use `crud` instead)
    # once users are exposed via API.
    user = models.User(name="other user", email="other@example.com")
    db.add(user)
    db.flush()
    db.refresh(user)

    return crud.obj_meta.create(
        db=db, obj_in=schemas.ObjectMetaCreate(notes="secret!"), user=user
    )


def test_api_object_meta_create_read(db_and_client_with_user_no_scopes):
    db, client, user = db_and_client_with_user_no_scopes
    notes = "test"
    grant_scope(db, user, ScopeType.META_WRITE)

    # Create new metadata.
    create_response = client.post(f"{META_ROOT}/", json={"notes": notes})
    assert create_response.status_code == HTTPStatus.CREATED
    create_body = schemas.ObjectMeta(**create_response.json())
    assert create_body.notes == notes
    assert create_body.created_by == user.email

    # Read it back.
    read_response = client.get(f"{META_ROOT}/{create_body.uuid}")
    assert read_response.status_code == HTTPStatus.OK
    read_body = schemas.ObjectMeta(**read_response.json())
    assert read_body == create_body


def test_api_object_meta_create__no_scopes(db_and_client_with_user_no_scopes):
    db, client, _ = db_and_client_with_user_no_scopes

    create_response = client.post(f"{META_ROOT}/", json={"notes": ""})
    assert create_response.status_code == HTTPStatus.FORBIDDEN
    assert "permissions to write metadata" in create_response.json()["detail"]


def test_api_object_meta_create__read_only(db_and_client_with_user_no_scopes):
    db, client, meta = db_and_client_with_user_no_scopes
    grant_scope(db, meta, ScopeType.META_READ)

    create_response = client.post(f"{META_ROOT}/", json={"notes": ""})
    assert create_response.status_code == HTTPStatus.FORBIDDEN
    assert "permissions to write metadata" in create_response.json()["detail"]


def test_api_object_meta_read__other_user_no_read_scope(
    db_and_client_with_user_no_scopes,
):
    db, client, user = db_and_client_with_user_no_scopes
    grant_scope(db, user, ScopeType.META_WRITE)
    other_user_meta = create_new_user_meta(db)

    # Read metadata created by the other user.
    read_response = client.get(f"{META_ROOT}/{other_user_meta.uuid}")
    assert read_response.status_code == HTTPStatus.FORBIDDEN
    assert "permissions to read metadata" in read_response.json()["detail"]


def test_api_object_meta_read__other_user_read_scope(db_and_client_with_user_no_scopes):
    db, client, user = db_and_client_with_user_no_scopes
    grant_scope(db, user, ScopeType.META_READ)
    other_user_meta = create_new_user_meta(db)

    # Read metadata created by the other user.
    read_response = client.get(f"{META_ROOT}/{other_user_meta.uuid}")
    assert read_response.status_code == HTTPStatus.OK
