from gerrydb_meta import crud, schemas
from gerrydb_meta.enums import ColumnKind, ColumnType


def test_crud_namespace_create(db_with_meta):
    db, meta = db_with_meta
    ns, _ = crud.namespace.create(
        db=db,
        obj_in=schemas.NamespaceCreate(
            path="atlantis",
            description="A legendary city",
            public=True,
        ),
        obj_meta=meta,
    )
    assert ns.path == "atlantis"
    assert ns.description == "A legendary city"
    assert ns.public is True


def test_crud_namespace_get(db_with_meta):
    db, meta = db_with_meta
    ns, _ = crud.namespace.create(
        db=db,
        obj_in=schemas.NamespaceCreate(
            path="atlantis",
            description="A legendary city",
            public=True,
        ),
        obj_meta=meta,
    )
    assert crud.namespace.get(db=db, path="atlantis").namespace_id == ns.namespace_id
