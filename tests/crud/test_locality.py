"""Tests for CherryDB CRUD operations on locality metadata."""
from cherrydb_meta import crud, schemas
from cherrydb_meta.crud.locality import normalize_path


def test_normalize_path_flat():
    assert normalize_path("atlantis") == "atlantis"


def test_normalize_path_case():
    assert normalize_path("Atlantis") == "atlantis"


def test_normalize_extra_slashes():
    assert normalize_path("/greece//atlantis") == "greece/atlantis"


def test_crud_locality_create_no_parent_no_aliases(db_with_meta):
    name = "Lost City of Atlantis"
    db, meta = db_with_meta
    loc = crud.locality.create(
        db=db,
        obj_in=schemas.LocalityCreate(
            canonical_path="atlantis",
            parent_path=None,
            name=name,
            aliases=None,
        ),
        obj_meta=meta,
    )
    assert loc.loc_id is not None
    assert loc.name == name


def test_crud_locality_get_by_ref(db_with_meta):
    name = "Lost City of Atlantis"
    db, meta = db_with_meta
    loc = crud.locality.create(
        db=db,
        obj_in=schemas.LocalityCreate(
            canonical_path="atlantis",
            parent_path=None,
            name=name,
            aliases=["greece/atlantis"],
        ),
        obj_meta=meta,
    )
    assert crud.locality.get_by_ref(db=db, path="atlantis").loc_id == loc.loc_id
    assert crud.locality.get_by_ref(db=db, path="greece/atlantis").loc_id == loc.loc_id


def test_crud_locality_patch(db_with_meta):
    name = "Lost City of Atlantis"
    db, meta = db_with_meta
    loc = crud.locality.create(
        db=db,
        obj_in=schemas.LocalityCreate(
            canonical_path="atlantis",
            parent_path=None,
            name=name,
            aliases=None,
        ),
        obj_meta=meta,
    )
    loc_with_aliases = crud.locality.patch(
        db=db,
        obj=loc,
        obj_meta=meta,
        patch=schemas.LocalityPatch(aliases=["greece/atlantis"]),
    )
    assert set(ref.path for ref in loc_with_aliases.refs) == {
        "atlantis",
        "greece/atlantis",
    }
