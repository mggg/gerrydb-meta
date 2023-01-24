"""Tests for CherryDB crud operations."""
from cherrydb_meta import crud, schemas


def test_crud_create_read_object_meta(db_with_user):
    db, user = db_with_user
    meta_create = crud.obj_meta.create(
        db=db,
        obj_in=schemas.ObjectMetaCreate(notes="metameta"),
        user=user,
    )
    meta_get = crud.obj_meta.get(db=db, id=meta_create.meta_id)
    assert meta_get.notes == meta_create.notes


def test_crud_create_location_no_parent_no_aliases(db_with_meta):
    db, meta = db_with_meta
    crud.location.create(
        db=db,
        obj_in=schemas.LocationCreate(
            canonical_path="atlantis",
            parent_path=None,
            name="Lost City of Atlantis",
            aliases=None,
        ),
        obj_meta=meta,
    )
