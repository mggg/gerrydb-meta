"""Tests for CherryDB CRUD operations on object metadata."""
from cherrydb_meta import crud, schemas


def test_crud_object_meta_create_read(db_with_user):
    db, user = db_with_user
    meta_create = crud.obj_meta.create(
        db=db,
        obj_in=schemas.ObjectMetaCreate(notes="metameta"),
        user=user,
    )
    meta_get = crud.obj_meta.get(db=db, id=meta_create.uuid)
    assert meta_get.notes == meta_create.notes
