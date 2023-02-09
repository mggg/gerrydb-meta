"""Endpoints for base geographic data (points and polygons)."""
from http import HTTPStatus
from typing import Callable

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from cherrydb_meta import crud, schemas, models
from cherrydb_meta.scopes import ScopeManager
from cherrydb_meta.api.base import NamespacedObjectApi
from cherrydb_meta.api.deps import get_db, get_geo_import, get_obj_meta, get_scopes


class GeographyApi(NamespacedObjectApi):
    def _create(self, router: APIRouter) -> Callable:
        @router.post(
            "/{namespace}/",
            response_model=self.get_schema,
            name=f"Create {self.obj_name_singular} or {self.obj_name_plural}",
            status_code=HTTPStatus.CREATED,
        )
        def create_route(
            *,
            namespace: str,
            obj_in: schemas.Geography | list[schemas.Geography],
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            geo_import: models.GeoImport = Depends(get_geo_import),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = self._namespace_with_write(
                db=db, scopes=scopes, path=namespace
            )
            geographies = [obj_in] if isinstance(obj_in, schemas.Geography) else obj_in
            self.crud.create_bulk()


router = GeographyApi(
    crud=crud.geo_import,
    get_schema=schemas.Geography,
    create_schema=None,
    obj_name_singular="Geography",
    obj_name_plural="Geographies",
).router()
