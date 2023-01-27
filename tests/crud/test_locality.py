"""Tests for CherryDB CRUD operations on locality metadata."""
from cherrydb_meta import crud, schemas


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
