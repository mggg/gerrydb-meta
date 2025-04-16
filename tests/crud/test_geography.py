from gerrydb_meta import crud, schemas
from gerrydb_meta.enums import ColumnKind, ColumnType
from gerrydb_meta import models
from gerrydb_meta.exceptions import *
from shapely import Point, Polygon
from shapely import wkb
from geoalchemy2 import WKBElement
import pytest

square_corners = [(-1, -1), (1, -1), (1, 1), (-1, 1)]

square = Polygon(square_corners)

internal_point = Point(0.0, 0.0)


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


def test_crud_geography_create_bulk(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

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

    assert geo[0][0].path == "central_atlantis"
    assert geo[1][0].path == "western_atlantis"


def test_crud_geography_create_bulk_redundant_fail(db_with_meta):
    with pytest.raises(BulkCreateError) as e:
        db, meta = db_with_meta
        ns = make_atlantis_ns(db, meta)

        geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

        geo, _ = crud.geography.create_bulk(
            db=db,
            objs_in=[
                schemas.GeographyCreate(
                    path="central_atlantis",
                    geography=None,
                    internal_point=None,
                ),
                schemas.GeographyCreate(
                    path="central_atlantis",
                    geography=square.wkb,
                    internal_point=None,
                ),
            ],
            obj_meta=meta,
            geo_import=geo_import,
            namespace=ns,
        )

    assert e.value.paths == ["central_atlantis", "central_atlantis"]
    assert str(e.value) == "Cannot create geographies with duplicate paths."


def test_crud_geography_create_bulk_already_exist_fail(db_with_meta):
    with pytest.raises(BulkCreateError) as e:
        db, meta = db_with_meta
        ns = make_atlantis_ns(db, meta)

        geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

        geo, _ = crud.geography.create_bulk(
            db=db,
            objs_in=[
                schemas.GeographyCreate(
                    path="central_atlantis",
                    geography=None,
                    internal_point=None,
                ),
            ],
            obj_meta=meta,
            geo_import=geo_import,
            namespace=ns,
        )

        geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

        geo, _ = crud.geography.create_bulk(
            db=db,
            objs_in=[
                schemas.GeographyCreate(
                    path="central_atlantis",
                    geography=None,
                    internal_point=None,
                ),
            ],
            obj_meta=meta,
            geo_import=geo_import,
            namespace=ns,
        )

    assert e.value.paths == ["central_atlantis"]
    assert str(e.value) == "Cannot create geographies that already exist."


def test_crud_geography_create_bulk_wkb_fail(db_with_meta):
    with pytest.raises(Exception) as e:
        db, meta = db_with_meta
        ns = make_atlantis_ns(db, meta)

        geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

        geo, _ = crud.geography.create_bulk(
            db=db,
            objs_in=[
                schemas.GeographyCreate(
                    path="central_atlantis", geography=b"4", internal_point=None
                ),
            ],
            geo_import=geo_import,
            namespace=ns,
            obj_meta=meta,
        )

    assert str(e.value) == (
        "Failed to insert geometries. This is likely due to invalid Geometries; please ensure "
        "geometries can be encoded in WKB format."
    )


def test_crud_geography_create_bulk_not_wkb_converted_fail(db_with_meta):
    with pytest.raises(Exception) as e:
        db, meta = db_with_meta
        ns = make_atlantis_ns(db, meta)

        geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

        geo, _ = crud.geography.create_bulk(
            db=db,
            objs_in=[
                schemas.GeographyCreate(
                    path="central_atlantis", geography=square, internal_point=None
                ),
            ],
            geo_import=geo_import,
            namespace=ns,
            obj_meta=meta,
        )
    assert str(e.value) == (
        "1 validation error for GeographyCreate\n"
        "geography\n"
        "  The geography must be of type bytes, got type Polygon (type=value_error)"
    )


def test_crud_geography_patch_bulk_vacuous_update(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

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

    geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

    geo, _ = crud.geography.patch_bulk(
        db=db,
        objs_in=[
            schemas.GeographyPatch(
                path="central_atlantis",
                geography=None,
                internal_point=None,
            ),
        ],
        geo_import=geo_import,
        namespace=ns,
    )

    assert geo[0][1].geography == WKBElement(Polygon().wkb, srid=4269)


def test_crud_geography_patch_bulk_redundant_update_fail(db_with_meta):
    with pytest.raises(BulkPatchError) as e:
        db, meta = db_with_meta

        ns = make_atlantis_ns(db, meta)

        geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

        geo, _ = crud.geography.create_bulk(
            db=db,
            objs_in=[
                schemas.GeographyCreate(
                    path="central_atlantis",
                    geography=None,
                    internal_point=None,
                ),
            ],
            obj_meta=meta,
            geo_import=geo_import,
            namespace=ns,
        )

        geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

        geo, _ = crud.geography.patch_bulk(
            db=db,
            objs_in=[
                schemas.GeographyPatch(
                    path="central_atlantis",
                    geography=None,
                    internal_point=None,
                ),
                schemas.GeographyPatch(
                    path="central_atlantis",
                    geography=None,
                    internal_point=None,
                ),
            ],
            geo_import=geo_import,
            namespace=ns,
        )
    assert e.value.paths == ["central_atlantis", "central_atlantis"]
    assert str(e.value) == "Cannot patch geographies with duplicate paths."


def test_crud_geography_patch_bulk_not_exist_fail(db_with_meta):
    with pytest.raises(BulkPatchError) as e:
        db, meta = db_with_meta

        ns = make_atlantis_ns(db, meta)

        geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

        geo, _ = crud.geography.create_bulk(
            db=db,
            objs_in=[
                schemas.GeographyCreate(
                    path="central_atlantis",
                    geography=None,
                    internal_point=None,
                ),
            ],
            obj_meta=meta,
            geo_import=geo_import,
            namespace=ns,
        )

        geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

        geo, _ = crud.geography.patch_bulk(
            db=db,
            objs_in=[
                schemas.GeographyPatch(
                    path="western_atlantis",
                    geography=None,
                    internal_point=None,
                ),
            ],
            geo_import=geo_import,
            namespace=ns,
        )
    assert e.value.paths == ["western_atlantis"]
    assert str(e.value) == "Cannot update geographies that do not exist."


def test_crud_geography_patch_bulk_all_squares(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

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

    geo_import, _ = crud.geo_import.create(db=db, obj_meta=meta, namespace=ns)

    geo, _ = crud.geography.patch_bulk(
        db=db,
        objs_in=[
            schemas.GeographyPatch(
                path="central_atlantis",
                geography=square.wkb,
                internal_point=internal_point.wkb,
            ),
            schemas.GeographyPatch(
                path="western_atlantis",
                geography=square.wkb,
                internal_point=internal_point.wkb,
            ),
        ],
        geo_import=geo_import,
        namespace=ns,
    )

    assert wkb.loads(geo[0][1].geography.desc) == square
