"""Endpoints for base geographic data (points and polygons)."""
from http import HTTPStatus
from typing import Callable

from fastapi import APIRouter, Depends, Response
from sqlalchemy.orm import Session

from gerrydb_meta import crud, models, schemas
from gerrydb_meta.api.base import (
    MsgpackResponse,
    MsgpackRoute,
    NamespacedObjectApi,
    add_etag,
)
from gerrydb_meta.api.deps import get_db, get_geo_import, get_obj_meta, get_scopes
from gerrydb_meta.scopes import ScopeManager


class GeographyApi(NamespacedObjectApi):
    def _create(self, router: APIRouter) -> Callable:
        @router.post(
            "/{namespace}",
            response_model=None,
            response_class=MsgpackResponse,
            name=f"Create {self.obj_name_singular} or {self.obj_name_plural}",
            status_code=HTTPStatus.CREATED,
        )
        def create_route(
            *,
            response: Response,
            namespace: str,
            raw_geographies: list[schemas.GeographyCreate],
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            geo_import: models.GeoImport = Depends(get_geo_import),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = self._namespace_with_write(
                db=db, scopes=scopes, path=namespace
            )
            geos, etag = self.crud.create_bulk(
                db=db,
                objs_in=raw_geographies,
                obj_meta=obj_meta,
                geo_import=geo_import,
                namespace=namespace_obj,
            )
            add_etag(response, etag)
            response_geos = [
                schemas.Geography(
                    path=geo.path,
                    geography=(
                        None
                        if geo_version.geography is None
                        else bytes(geo_version.geography.data)
                    ),
                    internal_point=(
                        None
                        if geo_version.internal_point is None
                        else bytes(geo_version.internal_point.data)
                    ),
                    namespace=namespace,
                    meta=obj_meta,
                    valid_from=geo_version.valid_from,
                ).dict()
                for geo, geo_version in geos
            ]
            return MsgpackResponse(response_geos, status_code=HTTPStatus.CREATED)

        return create_route

    def _patch(self, router: APIRouter) -> Callable:
        @router.patch(
            "/{namespace}",
            response_model=list[self.get_schema],
            response_class=MsgpackResponse,
            name=f"Patch {self.obj_name_singular} or {self.obj_name_plural}",
        )
        def patch_route(
            *,
            response: Response,
            namespace: str,
            raw_geographies: list[schemas.GeographyPatch],
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            geo_import: models.GeoImport = Depends(get_geo_import),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = self._namespace_with_write(
                db=db, scopes=scopes, path=namespace
            )
            geos, etag = self.crud.patch_bulk(
                db=db,
                objs_in=raw_geographies,
                geo_import=geo_import,
                namespace=namespace_obj,
            )
            add_etag(response, etag)
            response_geos = [
                schemas.Geography(
                    path=geo.path,
                    geography=(
                        None
                        if geo_version.geography is None
                        else bytes(geo_version.geography.data)
                    ),
                    internal_point=(
                        None
                        if geo_version.internal_point is None
                        else bytes(geo_version.internal_point.data)
                    ),
                    namespace=namespace,
                    meta=obj_meta,
                    valid_from=geo_version.valid_from,
                ).dict()
                for geo, geo_version in geos
            ]
            return MsgpackResponse(response_geos)

        return patch_route

    def router(self) -> APIRouter:
        """Generates a router with basic CR operations for geographies."""
        router = APIRouter()
        msgpack_router = APIRouter()
        msgpack_router.route_class = MsgpackRoute
        self._create(msgpack_router)
        self._patch(msgpack_router)
        self._get(router)
        self._all(router)
        router.include_router(msgpack_router)
        return router


router = GeographyApi(
    crud=crud.geography,
    get_schema=schemas.GeographyMeta,
    create_schema=None,
    obj_name_singular="Geography",
    obj_name_plural="Geographies",
).router()
