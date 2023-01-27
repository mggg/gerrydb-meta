"""Tests for CherryDB CRUD operations on object metadata."""
from cherrydb_meta import crud, schemas


def test_crud_object_meta_create_read(db_with_user):
    db, user = db_with_user
    meta_create = crud.obj_meta.create(
        db=db,
        obj_in=schemas.ObjectMetaCreate(notes="metameta"),
        user=user,
    )
    meta_get = crud.obj_meta.get(db=db, id=meta_create.meta_id)
    assert meta_get.notes == meta_create.notes


def test_crud_locality_create_read_no_parent_no_aliases(db_with_meta):
    db, meta = db_with_meta
    crud.locality.create(
        db=db,
        obj_in=schemas.LocalityCreate(
            canonical_path="atlantis",
            parent_path=None,
            name="Lost City of Atlantis",
            aliases=None,
        ),
        obj_meta=meta,
    )
