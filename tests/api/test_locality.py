"""Tests for CherryDB REST API locality endpoints."""
import pytest
from http import HTTPStatus

from cherrydb_meta import crud, schemas
from cherrydb_meta.enums import ScopeType
from cherrydb_meta.main import API_PREFIX
from .scopes import grant_scope, revoke_scope_type

LOCALITIES_ROOT = f"{API_PREFIX}/localities"


@pytest.fixture
def db_and_client_with_meta_locality(db_and_client_with_meta_no_scopes):
    """An API client with `LOCALITY_READ` and `LOCALITY_WRITE` scopes (+ session and metadata)."""
    db, client, meta = db_and_client_with_meta_no_scopes
    grant_scope(db, meta, ScopeType.LOCALITY_READ)
    grant_scope(db, meta, ScopeType.LOCALITY_WRITE)
    yield db, client, meta


@pytest.fixture
def client_with_meta_locality(db_and_client_with_meta_locality):
    """An API client with `LOCALITY_READ` and `LOCALITY_WRITE` scopes (+ session and metadata)."""
    _, client, meta = db_and_client_with_meta_locality
    yield client, meta


@pytest.fixture
def client_with_locality_read_only(db_and_client_with_meta_no_scopes):
    """An API client with `LOCALITY_READ` scope (+ a preexisting locality)."""
    canonical_path = "greece"
    db, client, meta = db_and_client_with_meta_no_scopes
    grant_scope(db, meta, ScopeType.LOCALITY_READ)

    crud.locality.create(
        db=db,
        obj_in=schemas.LocalityCreate(name="Greece", canonical_path=canonical_path),
        obj_meta=meta,
    )
    yield client, canonical_path


def test_api_locality_create_read__no_parent_no_aliases(client_with_meta_locality):
    client, meta = client_with_meta_locality
    name = "Lost City of Atlantis"
    path = "atlantis"

    # Create new locality.
    create_response = client.post(
        f"{LOCALITIES_ROOT}/", json={"name": name, "canonical_path": path}
    )
    assert create_response.status_code == HTTPStatus.CREATED
    create_body = schemas.Locality(**create_response.json())
    assert create_body.name == name
    assert create_body.canonical_path == path
    assert create_body.parent_path is None
    assert create_body.meta.uuid == str(meta.uuid)

    # Read it back.
    read_response = client.get(f"{API_PREFIX}/localities/{path}")
    assert read_response.status_code == HTTPStatus.OK
    read_body = schemas.Locality(**read_response.json())
    assert read_body == create_body


def test_api_locality_create_read__parent_and_aliases(client_with_meta_locality):
    client, meta = client_with_meta_locality
    name = "Lost City of Atlantis"
    path = "greece/atlantis"
    parent_path = "greece"
    aliases = ["atlantis", "g/atlantis"]

    # Create parent locality.
    create_parent_response = client.post(
        f"{LOCALITIES_ROOT}/", json={"name": "Greece", "canonical_path": parent_path}
    )
    assert create_parent_response.status_code == HTTPStatus.CREATED

    # Create child locality with aliases.
    create_child_response = client.post(
        f"{LOCALITIES_ROOT}/",
        json={
            "name": name,
            "canonical_path": path,
            "parent_path": parent_path,
            "aliases": aliases,
        },
    )
    assert create_child_response.status_code == HTTPStatus.CREATED
    create_child_body = schemas.Locality(**create_child_response.json())
    assert create_child_body.name == name
    assert create_child_body.canonical_path == path
    assert create_child_body.aliases == aliases
    assert create_child_body.parent_path == parent_path
    assert create_child_body.meta.uuid == str(meta.uuid)

    # Read back the parent and the child.
    read_response = client.get(f"{LOCALITIES_ROOT}/")
    assert read_response.status_code == HTTPStatus.OK
    read_body = [schemas.Locality(**obj) for obj in read_response.json()]
    assert set(loc.canonical_path for loc in read_body) == {path, parent_path}


def test_api_locality_create_read__with_redirects(client_with_meta_locality):
    client, _ = client_with_meta_locality
    name = "Lost City of Atlantis"
    path = "greece/atlantis"
    alias = "atlantis"

    # Create child locality with aliases.
    create_response = client.post(
        f"{LOCALITIES_ROOT}/",
        json={"name": name, "canonical_path": path, "aliases": [alias]},
    )
    assert create_response.status_code == HTTPStatus.CREATED

    read_response = client.get(f"{LOCALITIES_ROOT}/{alias}", follow_redirects=False)
    assert read_response.status_code == HTTPStatus.PERMANENT_REDIRECT


def test_api_locality_create__with_no_meta(db_and_client_with_user_no_scopes):
    db, client, user = db_and_client_with_user_no_scopes
    grant_scope(db, user, ScopeType.LOCALITY_WRITE)

    create_response = client.post(
        f"{LOCALITIES_ROOT}/", json={"name": "Greece", "canonical_path": "greece"}
    )
    assert create_response.status_code == HTTPStatus.BAD_REQUEST
    assert "metadata" in create_response.json()["detail"]


def test_api_locality_create__scope_read_only(db_and_client_with_meta_no_scopes):
    db, client, user = db_and_client_with_meta_no_scopes
    grant_scope(db, user, ScopeType.LOCALITY_READ)

    create_response = client.post(
        f"{LOCALITIES_ROOT}/", json={"name": "Greece", "canonical_path": "greece"}
    )
    assert create_response.status_code == HTTPStatus.FORBIDDEN
    assert "permissions to write" in create_response.json()["detail"]


def test_api_locality_create_read__scope_write_only(db_and_client_with_meta_no_scopes):
    db, client, user = db_and_client_with_meta_no_scopes
    grant_scope(db, user, ScopeType.LOCALITY_WRITE)

    create_response = client.post(
        f"{LOCALITIES_ROOT}/", json={"name": "Greece", "canonical_path": "greece"}
    )
    assert create_response.status_code == HTTPStatus.CREATED

    read_response = client.get(f"{LOCALITIES_ROOT}/greece")
    assert read_response.status_code == HTTPStatus.FORBIDDEN
    assert "permissions to read" in read_response.json()["detail"]


def test_api_locality_create__bad_parent_path(client_with_meta_locality):
    client, _ = client_with_meta_locality

    # Create child locality with aliases.
    create_response = client.post(
        f"{LOCALITIES_ROOT}/",
        json={
            "name": "Lost City of Atlantis",
            "canonical_path": "greece/atlantis",
            "parent_path": "greece",
        },
    )
    assert create_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert "parent" in create_response.json()["detail"]


def test_api_locality__create_twice(client_with_meta_locality):
    client, _ = client_with_meta_locality
    body = {
        "name": "Lost City of Atlantis",
        "canonical_path": "greece/atlantis",
    }

    create_response = client.post(f"{LOCALITIES_ROOT}/", json=body)
    assert create_response.status_code == HTTPStatus.CREATED

    create_again_response = client.post(f"{LOCALITIES_ROOT}/", json=body)
    assert create_again_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert "exist" in create_again_response.json()["detail"]

    read_response = client.get(f"{LOCALITIES_ROOT}/")
    assert read_response.status_code == HTTPStatus.OK
    assert len(read_response.json()) == 1


def test_api_locality_patch__add_aliases(client_with_meta_locality):
    client, _ = client_with_meta_locality
    path = "greece/atlantis"
    aliases = ["atlantis", "g/atlantis"]

    create_response = client.post(
        f"{LOCALITIES_ROOT}/",
        json={"name": "Lost City of Atlantis", "canonical_path": path},
    )
    assert create_response.status_code == HTTPStatus.CREATED

    patch_response = client.patch(
        f"{LOCALITIES_ROOT}/{path}", json={"aliases": aliases[:1]}
    )
    assert patch_response.status_code == HTTPStatus.OK

    patch_again_response = client.patch(
        f"{LOCALITIES_ROOT}/{path}", json={"aliases": aliases}
    )
    assert patch_again_response.status_code == HTTPStatus.OK
    patch_body = schemas.Locality(**patch_again_response.json())
    assert set(patch_body.aliases) == set(aliases)


def test_api_locality_patch__read_only(client_with_locality_read_only):
    client, path = client_with_locality_read_only

    patch_response = client.patch(
        f"{LOCALITIES_ROOT}/{path}", json={"aliases": [path + "_alias"]}
    )
    assert patch_response.status_code == HTTPStatus.FORBIDDEN
    assert "permissions to write" in patch_response.json()["detail"]
