"""Generic CR(UD) views and utilities."""
import inspect
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Callable, Type
from uuid import UUID

import ormsgpack as msgpack
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.routing import APIRoute
from pydantic import BaseModel
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models
from cherrydb_meta.api.deps import get_db, get_obj_meta, get_scopes
from cherrydb_meta.scopes import ScopeManager


def check_etag(db: Session, crud_obj: crud.CRBase, header: str) -> None:
    """Processes an `If-None-Match` header.

    Raises 304 Not Modified if the collection's current ETag
    matches the ETag in `header`. Otherwise, does nothing.
    """
    etag = crud_obj.etag(db=db)
    if etag is not None and header == '"{etag}"':
        raise HTTPException(status_code=HTTPStatus.NOT_MODIFIED)


def check_namespaced_etag(
    db: Session,
    crud_obj: crud.NamespacedCRBase,
    namespace: models.Namespace,
    header: str,
):
    """Processes an `If-None-Match` header.

    Raises 304 Not Modified if the namespaced collection's current ETag
    matches the ETag in `header`. Otherwise, does nothing.
    """
    etag = crud_obj.etag(db=db, namespace=namespace)
    if etag is not None and header == '"{etag}"':
        raise HTTPException(status_code=HTTPStatus.NOT_MODIFIED)


def add_etag(response: Response, etag: UUID | None) -> None:
    """Adds an `ETag` header to a response from a UUID."""
    if etag is not None:
        response.headers["ETag"] = f'"{etag}"'


def namespace_read_error_msg(obj_name: str) -> str:
    """Generates an error message for a failed read in a namespace."""
    return (
        "Namespace not found, or you do not have sufficient permissions "
        f"to read {obj_name.lower()} in this namespace."
    )


def namespace_write_error_msg(obj_name: str) -> str:
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


# see https://fastapi.tiangolo.com/advanced/custom-request-and-route/
class MsgpackRequest(Request):
    """A request with a MessagePack-encoded body."""

    async def body(self) -> bytes:
        if not hasattr(self, "_body"):
            body = await super().body()
            if body:
                body = msgpack.unpackb(body)
            self._body = body
        return self._body


# see https://fastapi.tiangolo.com/advanced/custom-response/
class MsgpackResponse(Response):
    """A request with a MessagePack-encoded body."""

    media_type = "application/msgpack"

    def render(self, content: Any) -> bytes:
        return msgpack.packb(content)


class MsgpackRoute(APIRoute):
    """A route where requests must have a MessagePack-encoded body."""

    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            if request.headers.get("content-type") not in (
                "application/msgpack",
                "application/x-msgpack",
            ):
                raise HTTPException(
                    status_code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                    detail="Only MessagePack requests are supported by this endpoint.",
                )

            try:
                request = MsgpackRequest(request.scope, request.receive)
                return await original_route_handler(request)
            except msgpack.MsgpackDecodeError as ex:
                raise ex
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail="Request body is not a valid MessagePack object.",
                )

        return custom_route_handler


@dataclass
class NamespacedObjectApi:
    """Generic API for a namespaced object."""

    crud: crud.NamespacedCRBase
    get_schema: crud.GetSchemaType
    create_schema: crud.CreateSchemaType | None
    obj_name_singular: str
    obj_name_plural: str
    patch_schema: crud.PatchSchemaType | None = None

    def _namespace_with_read(
        self, *, db: Session, scopes: ScopeManager, path: str
    ) -> models.Namespace:
        """Loads a namespace with read access or raises an HTTP error."""
        namespace_obj = crud.namespace.get(db=db, path=path)
        if namespace_obj is None or not scopes.can_read_in_namespace(namespace_obj):
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=namespace_read_error_msg(self.obj_name_plural),
            )
        return namespace_obj

    def _namespace_with_write(
        self, *, db: Session, scopes: ScopeManager, path: str
    ) -> models.Namespace:
        """Loads a namespace with write access or raises an HTTP error."""
        namespace_obj = crud.namespace.get(db=db, path=path)
        if namespace_obj is None or not scopes.can_write_in_namespace(namespace_obj):
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=namespace_write_error_msg(self.obj_name_plural),
            )
        return namespace_obj

    def _obj(
        self, *, db: Session, namespace: models.Namespace, path: str
    ) -> models.DeclarativeBase:
        """Loads a generic namespaced object or raises an HTTP error."""
        obj = self.crud.get(db=db, namespace=namespace, path=path)
        if obj is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f"{self.obj_name_singular} not found in namespace.",
            )
        return obj

    def _check_etag(
        self, *, db: Session, namespace: models.Namespace, header: str | None
    ) -> None:
        """Processes an `If-None-Match` header.

        Raises 304 Not Modified if the namespaced collection's current ETag
        matches the ETag in `header`. Otherwise, does nothing.
        """
        check_namespaced_etag(
            db=db, crud_obj=self.crud, namespace=namespace, header=header
        )

    def _get(self, router: APIRouter) -> Callable:
        @router.get(
            "/{namespace}/{path:path}",
            response_model=self.get_schema,
            name=f"Read {self.obj_name_singular}",
        )
        def get_route(
            *,
            response: Response,
            namespace: str,
            path: str,
            db: Session = Depends(get_db),
            scopes: ScopeManager = Depends(get_scopes),
            if_none_match: str | None = Header(default=None),
        ):
            namespace_obj = self._namespace_with_read(
                db=db, scopes=scopes, path=namespace
            )
            self._check_etag(db=db, namespace=namespace_obj, header=if_none_match)
            etag = self.crud.etag(db, namespace_obj)
            obj = self._obj(db=db, namespace=namespace_obj, path=path)
            add_etag(response, etag)
            return obj

        return get_route

    def _all(self, router: APIRouter) -> Callable:
        @router.get(
            "/{namespace}",
            response_model=list[self.get_schema],
            name=f"Read {self.obj_name_plural}",
        )
        def all_route(
            *,
            response: Response,
            namespace: str,
            db: Session = Depends(get_db),
            scopes: ScopeManager = Depends(get_scopes),
            if_none_match: str | None = Header(default=None),
        ):
            namespace_obj = self._namespace_with_read(
                db=db, scopes=scopes, path=namespace
            )
            self._check_etag(db=db, namespace=namespace_obj, header=if_none_match)
            etag = self.crud.etag(db, namespace_obj)
            objs = self.crud.all_in_namespace(db=db, namespace=namespace_obj)
            add_etag(response, etag)
            return objs

        return all_route

    def _create(self, router: APIRouter) -> Callable:
        @router.post(
            "/{namespace}",
            response_model=self.get_schema,
            name=f"Create {self.obj_name_singular}",
            status_code=HTTPStatus.CREATED,
        )
        @body_schema(self.create_schema)
        def create_route(
            *,
            response: Response,
            namespace: str,
            obj_in: BaseModel,
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = self._namespace_with_write(
                db=db, scopes=scopes, path=namespace
            )
            obj, etag = self.crud.create(
                db=db, obj_in=obj_in, namespace=namespace_obj, obj_meta=obj_meta
            )
            add_etag(response, etag)
            return obj

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
            response: Response,
            namespace: str,
            path: str,
            obj_in: BaseModel,
            db: Session = Depends(get_db),
            obj_meta: models.ObjectMeta = Depends(get_obj_meta),
            scopes: ScopeManager = Depends(get_scopes),
        ):
            namespace_obj = self._namespace_with_write(
                db=db, scopes=scopes, path=namespace
            )
            obj = self._obj(db=db, namespace=namespace_obj, path=path)
            patched_obj, etag = self.crud.patch(
                db=db, obj=obj, obj_meta=obj_meta, patch=obj_in
            )
            add_etag(response, etag)
            return patched_obj

        return patch_route

    def router(self) -> APIRouter:
        """Generates a router with basic CR operations for the object."""
        router = APIRouter()
        for route_func in (self._get, self._all, self._create):
            route_func(router)

        if self.patch_schema is not None and hasattr(self.crud, "patch"):
            self._patch(router)

        return router
