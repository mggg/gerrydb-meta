"""Tests for CherryDB REST API locality endpoints."""
from http import HTTPStatus

from cherrydb_meta import schemas
from cherrydb_meta.main import API_PREFIX

LOCALITIES_ROOT = f"{API_PREFIX}/localities"


def test_api_locality_create_read_no_parent_no_aliases(client_with_meta):
    client, meta = client_with_meta
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
    assert create_child_body.meta.meta_id == meta.meta_id

    # Read back the parent and the child.
    read_response = client.get(f"{LOCALITIES_ROOT}/")
    assert read_response.status_code == HTTPStatus.OK
    read_body = [schemas.Locality(**obj) for obj in read_response.json()]
    assert set(loc.canonical_path for loc in read_body) == {path, parent_path}


def test_api_locality_create_read_with_redirects(client_with_meta):
    client, _ = client_with_meta
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


def test_api_locality_create_with_no_meta(client_with_user):
    client, _ = client_with_user

    # Create child locality with aliases.
    create_response = client.post(
        f"{LOCALITIES_ROOT}/", json={"name": "Greece", "canonical_path": "greece"}
    )
    assert create_response.status_code == HTTPStatus.BAD_REQUEST
    assert "metadata" in create_response.json()["detail"]


def test_api_locality_create_bad_parent_path(client_with_meta):
    client, _ = client_with_meta

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


def test_api_locality_create_twice(client_with_meta):
    client, _ = client_with_meta
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


def test_api_locality_patch_add_aliases(client_with_meta):
    client, _ = client_with_meta
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
