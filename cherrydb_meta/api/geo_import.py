"""Endpoints for geographic layer metadata."""
from http import HTTPStatus
from typing import Callable
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.base import NamespacedObjectApi
from cherrydb_meta.api.deps import get_db, get_obj_meta, get_scopes
from cherrydb_meta.scopes import ScopeManager


class GeoImportApi(NamespacedObjectApi):
    def _obj(
        self, *, db: Session, namespace: models.Namespace, uuid: str
    ) -> models.DeclarativeBase:
        """Loads a generic namespaced object by UUID or raises an HTTP error."""
        try:
            parsed_uuid = UUID(uuid)
        except ValueError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail="GeoImport ID is not a valid UUID hex string.",
            )

        obj = self.crud.get(db=db, namespace=namespace, uuid=parsed_uuid)
        if obj is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f"{self.obj_name_singular} not found in namespace.",
            )

    def _get(self, router: APIRouter) -> Callable:
        @router.get(
            "/{namespace}/{uuid}",
            response_model=self.get_schema,
            name=f"Read {self.obj_name_singular}",
        )
        def get_route(
            *,
            namespace: str,
            uuid: str,
            db: Session = Depends(get_db),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = self._namespace_with_read(
                db=db, scopes=scopes, path=namespace
            )
            return self._obj(db=db, namespace=namespace_obj, uuid=uuid)

        return get_route

    def _create(self, router: APIRouter) -> Callable:
        @router.post(
            "/{namespace}",
            response_model=self.get_schema,
            name=f"Create {self.obj_name_singular}",
            status_code=HTTPStatus.CREATED,
        )
        def create_route(
            *,
            namespace: str,
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = self._namespace_with_write(
                db=db, scopes=scopes, path=namespace
            )
            return self.crud.create(db=db, namespace=namespace_obj, obj_meta=obj_meta)

        return create_route


router = GeoImportApi(
    crud=crud.geo_import,
    get_schema=schemas.GeoImport,
    create_schema=None,
    obj_name_singular="GeoImport",
    obj_name_plural="GeoImports",
).router()
