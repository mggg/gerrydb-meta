"""Endpoints for base geographic data (points and polygons)."""
from http import HTTPStatus
from typing import Any, Callable

import msgpack
import shapely.wkb
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.base import MsgpackRoute, NamespacedObjectApi
from cherrydb_meta.api.deps import get_db, get_geo_import, get_obj_meta, get_scopes
from cherrydb_meta.scopes import ScopeManager


class GeographyApi(NamespacedObjectApi):
    def _create(self, router: APIRouter) -> Callable:
        @router.post(
            "/{namespace}",
            response_model=self.get_schema,
            name=f"Create {self.obj_name_singular} or {self.obj_name_plural}",
            status_code=HTTPStatus.CREATED,
        )
        def create_route(
            *,
            namespace: str,
            obj_in: schemas.GeographyCreate | list[schemas.GeographyCreate],
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            geo_import: models.GeoImport = Depends(get_geo_import),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = self._namespace_with_write(
                db=db, scopes=scopes, path=namespace
            )
            raw_geographies = (
                [obj_in] if isinstance(obj_in, schemas.GeographyCreate) else obj_in
            )
            self.crud.create_bulk(
                db=db,
                objs_in=raw_geographies,
                obj_meta=obj_meta,
                geo_import=geo_import,
                namespace=namespace_obj,
            )

            return create_route

    def router(self) -> APIRouter:
        """Generates a router with basic CR operations for geographies."""
        router = APIRouter()
        msgpack_router = APIRouter()
        msgpack_router.route_class = MsgpackRoute
        self._create(msgpack_router)
        self._get(router)
        router.include_router(msgpack_router)
        return router


router = GeographyApi(
    crud=crud.geography,
    get_schema=schemas.Geography,
    create_schema=None,
    obj_name_singular="Geography",
    obj_name_plural="Geographies",
).router()
