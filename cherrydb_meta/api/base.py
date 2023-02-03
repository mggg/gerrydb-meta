"""Generic CR(UD) views for namespaced objects."""
import inspect
from dataclasses import dataclass
from pydantic import BaseModel
from http import HTTPStatus
from typing import Callable, Type

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from cherrydb_meta import crud, models
from cherrydb_meta.scopes import ScopeManager
from cherrydb_meta.api.deps import get_db, get_obj_meta, get_scopes


def _namespace_read_error_msg(obj_name: str) -> str:
    """Generates an error message for a failed read in a namespace."""
    return (
        "Namespace not found, or you do not have sufficient permissions "
        f"to read {obj_name.lower()} in this namespace."
    )


def _namespace_write_error_msg(obj_name: str) -> str:
    """Generates an error message for a failed write in a namespace."""
    return (
        "Namespace not found, or you do not have sufficient permissions "
        f"to write {obj_name.lower()} in this namespace."
    )


def body_schema(obj_type: Type, obj_arg: str = "obj_in") -> Callable:
    """Injects a schema type into the signature of a request handler.

    FastAPI derives validation logic and API documentation from the type
    annotations of request handlers, so it does not suffice to use the `BaseModel`
    class or similar as the type annotation for a request body (conventionally
    the `obj_in` argument to a handler). Thus, we mutate a handler's signature
    in place to provide a more specific type annotation before registering it
    with an API router.
    """

    def decorator(func: Callable) -> Callable:
        signature = inspect.signature(func)
        params = dict(signature.parameters)
        params[obj_arg] = params[obj_arg].replace(annotation=obj_type)
        func.__signature__ = signature.replace(parameters=params.values())
        return func

    return decorator


@dataclass
class NamespacedObjectApi:
    """Generic API for a namespaced object."""

    crud: crud.NamespacedCRBase
    get_schema: crud.GetSchemaType
    create_schema: crud.CreateSchemaType
    obj_name_singular: str
    obj_name_plural: str
    patch_schema: crud.PatchSchemaType | None = None

    def _get(self, router: APIRouter) -> Callable:
        @router.get(
            "/{namespace}/{path:path}",
            response_model=self.get_schema,
            name=f"Read {self.obj_name_singular}",
        )
        def get_route(
            *,
            namespace: str,
            path: str,
            db: Session = Depends(get_db),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = crud.namespace.get(db=db, path=namespace)
            if namespace_obj is None or not scopes.can_read_in_namespace(namespace_obj):
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail=_namespace_read_error_msg(self.obj_name_plural),
                )

            obj = self.crud.get(db=db, namespace=namespace_obj, path=path)
            if obj is None:
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail=f"{self.obj_name_singulars} not found in namespace.",
                )
            return obj

        return get_route

    def _all(self, router: APIRouter) -> Callable:
        @router.get(
            "/{namespace}/",
            response_model=list[self.get_schema],
            name=f"Read {self.obj_name_plural}",
        )
        def all_route(
            *,
            namespace: str,
            db: Session = Depends(get_db),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = crud.namespace.get(db=db, path=namespace)
            if namespace_obj is None or not scopes.can_read_in_namespace(namespace_obj):
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail=_namespace_write_error_msg(self.obj_name_plural),
                )
            return self.crud.all(db=db, namespace=namespace_obj)

        return all_route

    def _create(self, router: APIRouter) -> Callable:
        @router.post(
            "/{namespace}/",
            response_model=self.get_schema,
            name=f"Create {self.obj_name_singular}",
            status_code=HTTPStatus.CREATED,
        )
        @body_schema(self.create_schema)
        def create_route(
            *,
            namespace: str,
            obj_in: BaseModel,
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = crud.namespace.get(db=db, path=namespace)
            if namespace_obj is None or not scopes.can_write_in_namespace(
                namespace_obj
            ):
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail=_namespace_write_error_msg(self.obj_name_plural),
                )
            return self.crud.create(
                db=db, obj_in=obj_in, namespace=namespace_obj, obj_meta=obj_meta
            )

        return create_route

    def _patch(self, router: APIRouter) -> Callable:
        @router.patch(
            "/{namespace}/{path:path}",
            response_model=self.get_schema,
            name=f"Patch {self.obj_name_singular}",
        )
        @body_schema(self.patch_schema)
        def patch_route(
            *,
            namespace: str,
            path: str,
            obj_in: BaseModel,
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = crud.namespace.get(db=db, path=namespace)
            if namespace_obj is None or not scopes.can_write_in_namespace(
                namespace_obj
            ):
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail=_namespace_write_error_msg(self.obj_name_plural),
                )

            obj = self.crud.get(db=db, namespace=namespace_obj, path=path)
            if obj is None:
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail=f"{self.obj_name_singular} not found in namespace.",
                )
            return self.crud.patch(db=db, obj=obj, obj_meta=obj_meta, patch=obj_in)

        return patch_route

    def router(self) -> APIRouter:
        """Generates a router with basic CR operations for the object."""
        router = APIRouter()
        for route_func in (self._get, self._all, self._create):
            route_func(router)

        if self.patch_schema is not None and hasattr(self.crud, "patch"):
            self._patch(router)

        return router
