import networkx as nx
from gerrydb_meta import crud, schemas, models
from gerrydb_meta.enums import ColumnKind, ColumnType
from gerrydb_meta import models
from shapely import Point, Polygon
from shapely import wkb
import pytest
from gerrydb_meta.crud.base import NamespacedCRBase, normalize_path


square_corners = [(-1,-1), (1,-1), (1,1), (-1, 1)]

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


def test_plan_create(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    geo_layer, _ = crud.geo_layer.create(
        db=db,
        obj_in=schemas.GeoLayerCreate(
            path="atlantis_layer",
            description="The legendary city of Atlantis",
            source_url="https://en.wikipedia.org/wiki/Atlantis",
        ),
        obj_meta=meta,
        namespace=ns,
    )

    loc, _ = crud.locality.create_bulk(
        db=db,
        objs_in=[
            schemas.LocalityCreate(
                canonical_path="atlantis_loc",
                parent_path=None,
                name="Locality of the Lost City of Atlantis",
                aliases=None,
            ),
        ],
        obj_meta=meta,
    )
    
    geo_import, _ = crud.geo_import.create(
        db=db,
        obj_meta=meta,
        namespace=ns
    )
    
    geo, _ = crud.geography.create_bulk(
        db=db,
        objs_in=[
            schemas.GeographyCreate(
                path="central_atlantis",
                geography = None,
                internal_point = None,
            ),
            schemas.GeographyCreate(
                path="western_atlantis",
                geography = None,
                internal_point = None,
            ),
        ],
        obj_meta=meta,
        geo_import=geo_import,
        namespace=ns
    )
    
    geography_list = [geo[0] for geo in geo]
    
    crud.geo_layer.map_locality(
        db=db,
        layer=geo_layer,
        locality=loc[0],
        geographies=geography_list,
        obj_meta=meta
    )
    
    
    geo_set_version = crud.geo_layer.get_set_by_locality(
        db=db,
        layer=geo_layer,
        locality=loc[0]
    )

    plan, _ = crud.plan.create(
        db=db,
        obj_in=schemas.PlanCreate(
            path="atlantis_plan",
            description="A plan for the city of Atlantis",
            source_url="https://en.wikipedia.org/wiki/Atlantis",
            districtr_id="districtr_atlantis_plan",
            daves_id="daves_atlantis_plan",
            locality="atlantis_loc",
            layer="atlantis_layer",
            assignments={"central_atlantis": "1", "western_atlantis": "2"}
        ),
        geo_set_version=geo_set_version,
        obj_meta=meta,
        namespace=ns,
        assignments={geo[0][0]: "1", geo[1][0]: "2"},
    )

    assert plan.num_districts == 2
    assert set([(item.geo.full_path, item.assignment) for item in plan.assignments]) == set([("/atlantis/central_atlantis", "1"), ("/atlantis/western_atlantis", "2")])
    


def test_plan_get(db_with_meta):
    db, meta = db_with_meta

    ns = make_atlantis_ns(db, meta)

    geo_layer, _ = crud.geo_layer.create(
        db=db,
        obj_in=schemas.GeoLayerCreate(
            path="atlantis_layer",
            description="The legendary city of Atlantis",
            source_url="https://en.wikipedia.org/wiki/Atlantis",
        ),
        obj_meta=meta,
        namespace=ns,
    )

    loc, _ = crud.locality.create_bulk(
        db=db,
        objs_in=[
            schemas.LocalityCreate(
                canonical_path="atlantis_loc",
                parent_path=None,
                name="Locality of the Lost City of Atlantis",
                aliases=None,
            ),
        ],
        obj_meta=meta,
    )
    
    geo_import, _ = crud.geo_import.create(
        db=db,
        obj_meta=meta,
        namespace=ns
    )
    
    geo, _ = crud.geography.create_bulk(
        db=db,
        objs_in=[
            schemas.GeographyCreate(
                path="central_atlantis",
                geography = None,
                internal_point = None,
            ),
            schemas.GeographyCreate(
                path="western_atlantis",
                geography = None,
                internal_point = None,
            ),
        ],
        obj_meta=meta,
        geo_import=geo_import,
        namespace=ns
    )
    
    geography_list = [geo[0] for geo in geo]
    
    crud.geo_layer.map_locality(
        db=db,
        layer=geo_layer,
        locality=loc[0],
        geographies=geography_list,
        obj_meta=meta
    )
    
    
    geo_set_version = crud.geo_layer.get_set_by_locality(
        db=db,
        layer=geo_layer,
        locality=loc[0]
    )

    plan, _ = crud.plan.create(
        db=db,
        obj_in=schemas.PlanCreate(
            path="atlantis_plan",
            description="A plan for the city of Atlantis",
            source_url="https://en.wikipedia.org/wiki/Atlantis",
            districtr_id="districtr_atlantis_plan",
            daves_id="daves_atlantis_plan",
            locality="atlantis_loc",
            layer="atlantis_layer",
            assignments={"central_atlantis": "1", "western_atlantis": "2"}
        ),
        geo_set_version=geo_set_version,
        obj_meta=meta,
        namespace=ns,
        assignments={geo[0][0]: "1", geo[1][0]: "2"},
    )
    
    retrieved_plan = crud.plan.get(
        db=db,
        path="atlantis_plan",
        namespace=ns
    )
    
    assert retrieved_plan == plan