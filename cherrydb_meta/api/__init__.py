"""Base configuration and routing for Cherry API endpoints."""
from fastapi import APIRouter

from cherrydb_meta.api import geo_import, locality, namespace, obj_meta
from cherrydb_meta.api.base import NamespacedObjectApi
from cherrydb_meta import crud, schemas

api_router = APIRouter()
api_router.include_router(locality.router, prefix="/localities", tags=["localities"])
api_router.include_router(namespace.router, prefix="/namespaces", tags=["namespaces"])
api_router.include_router(obj_meta.router, prefix="/meta", tags=["meta"])
api_router.include_router(
    geo_import.router, prefix="/geo-imports", tags=["geo-imports"]
)

api_router.include_router(
    NamespacedObjectApi(
        crud=crud.column,
        get_schema=schemas.Column,
        create_schema=schemas.ColumnCreate,
        patch_schema=schemas.ColumnPatch,
        obj_name_singular="Column",
        obj_name_plural="Columns",
    ).router(),
    prefix="/columns",
    tags=["columns"],
)

api_router.include_router(
    NamespacedObjectApi(
        crud=crud.column_set,
        get_schema=schemas.ColumnSet,
        create_schema=schemas.ColumnSetCreate,
        obj_name_singular="ColumnSet",
        obj_name_plural="ColumnSets",
    ).router(),
    prefix="/column-sets",
    tags=["column-sets"],
)

api_router.include_router(
    NamespacedObjectApi(
        crud=crud.geo_layer,
        get_schema=schemas.GeoLayer,
        create_schema=schemas.GeoLayerCreate,
        obj_name_singular="GeoLayer",
        obj_name_plural="GeoLayers",
    ).router(),
    prefix="/layers",
    tags=["layers"],
)
