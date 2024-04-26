"""Tests for GerryDB CRUD operations on column metadata."""

from gerrydb_meta import crud, schemas
from gerrydb_meta.enums import ColumnKind, ColumnType
from gerrydb_meta import models


def make_atlantis_ns(db, meta):
    ns, _ = crud.namespace.create(
        db=db,
        obj_in=schemas.NamespaceCreate(
            path="atlantis",
            description="A legendary city",
            public=True,
        ),
        obj_meta=meta,
    )
    return ns


def test_crud_column_create_base(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    col, _ = crud.column.create(
        db=db,
        obj_in=schemas.ColumnCreate(
            canonical_path="mayor",
            description="the mayor of the city",
            kind=ColumnKind.IDENTIFIER,
            type=ColumnType.STR,
        ),
        obj_meta=meta,
        namespace=ns,
    )

    assert col.col_id is not None
    assert col.description == "the mayor of the city"
    assert col.kind == ColumnKind.IDENTIFIER
    assert col.type == ColumnType.STR

    assert col.canonical_ref.path == "mayor"
    assert col.canonical_ref.namespace_id == ns.namespace_id
    assert col.canonical_ref.meta_id == meta.meta_id


def test_crud_column_get_ref(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    col, _ = crud.column.create(
        db=db,
        obj_in=schemas.ColumnCreate(
            canonical_path="mayor",
            description="the mayor of the city",
            kind=ColumnKind.IDENTIFIER,
            type=ColumnType.STR,
        ),
        obj_meta=meta,
        namespace=ns,
    )
    

    assert crud.column.get_ref(db=db, path="mayor", namespace=ns).col_id == col.col_id
    assert crud.column.get_ref(db=db, path="mayor", namespace=ns) is col.canonical_ref


def test_crud_column_get(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    col, _ = crud.column.create(
        db=db,
        obj_in=schemas.ColumnCreate(
            canonical_path="mayor",
            description="the mayor of the city",
            kind=ColumnKind.IDENTIFIER,
            type=ColumnType.STR,
        ),
        obj_meta=meta,
        namespace=ns,
    )

    assert crud.column.get(db=db, path="mayor", namespace=ns).col_id == col.col_id
    assert crud.column.get(db=db, path="mayor", namespace=ns) is col


def test_crud_column_get_global_ref(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    col, _ = crud.column.create(
        db=db,
        obj_in=schemas.ColumnCreate(
            canonical_path="mayor",
            description="the mayor of the city",
            kind=ColumnKind.IDENTIFIER,
            type=ColumnType.STR,
        ),
        obj_meta=meta,
        namespace=ns,
    )

    assert (
        crud.column.get_global_ref(
            db=db, path=("atlantis", "mayor"), namespace=ns
        ).col_id
        == col.col_id
    )
    assert (
        crud.column.get_global_ref(db=db, path=("atlantis", "mayor"), namespace=ns)
        is col.canonical_ref
    )
    assert (
        crud.column.get_global_ref(
            db=db, path=("atlantis", "mayor"), namespace=ns
        ).namespace_id
        == ns.namespace_id
    )
    assert (
        crud.column.get_global_ref(
            db=db, path=("atlantis", "mayor"), namespace=ns
        ).meta_id
        == meta.meta_id
    )
    assert (
        crud.column.get_global_ref(db=db, path=("atlantis", "mayor"), namespace=ns).path
        == "mayor"
    )


def test_crud_column_set_values(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

    # create bulk returns a list of geos and a uuid
    # The geos are (Geography, GeoVersion) pairs
    geo, _ = crud.geography.create_bulk(
        db=db,
        objs_in=[
            schemas.GeographyCreate(
                path="central_atlantis",
                geography=None,
                internal_point=None,
            ),
            schemas.GeographyCreate(
                path="western_atlantis",
                geography=None,
                internal_point=None,
            ),
        ],
        obj_meta=meta,
        geo_import=geo_import,
        namespace=ns,
    )

    geo_update_values = []
    for i, g in enumerate(geo):
        geo_update_values.append((g[0], 100 + i))

    col, _ = crud.column.create(
        db=db,
        obj_in=schemas.ColumnCreate(
            canonical_path="geo identifier",
            description="an identifier number for the region",
            kind=ColumnKind.IDENTIFIER,
            type=ColumnType.INT,
        ),
        obj_meta=meta,
        namespace=ns,
    )

    crud.column.set_values(
        db=db,
        col=col,
        values=geo_update_values,
        obj_meta=meta,
    )

    # print(dir(crud.column.get(db=db, path="geo identifier", namespace=ns)))
    cols_list = db.query(models.ColumnValue).all()
    assert len(cols_list) == 2
    assert cols_list[0].val_int == 100
    assert cols_list[1].val_int == 101

    assert cols_list[0].geo_id == geo[0][0].geo_id
    assert cols_list[1].geo_id == geo[1][0].geo_id

    assert cols_list[0].col_id == col.col_id
    assert cols_list[1].col_id == col.col_id

    assert cols_list[0].val_float is None
    assert cols_list[1].val_float is None

    assert cols_list[0].val_str is None
    assert cols_list[1].val_str is None

    assert cols_list[0].val_bool is None
    assert cols_list[1].val_bool is None

    assert cols_list[0].val_json is None
    assert cols_list[1].val_json is None

def test_crud_column_patch(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    col, _ = crud.column.create(
        db=db,
        obj_in=schemas.ColumnCreate(
            canonical_path="geo identifier",
            description="an identifier number for the region",
            kind=ColumnKind.IDENTIFIER,
            type=ColumnType.INT,
            aliases=None,
        ),
        obj_meta=meta,
        namespace=ns,
    )

    data_col, _ = crud.column.patch(
        db=db,
        obj=col,
        obj_meta=meta,
        patch=schemas.ColumnPatch(aliases=['foo/bar']),
    )

    refs_lst = []
    for col_ref in data_col.refs:
        refs_lst.append(col_ref.path)
    
    assert "foo/bar" in refs_lst
    assert "geo identifier" in refs_lst
    