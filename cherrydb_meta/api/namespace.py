"""Endpoints for namespace metadata."""
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.scopes import ScopeManager
from cherrydb_meta.api.deps import (get_db, get_obj_meta, get_scopes, can_create_namespace)

router = APIRouter()


@router.get("/", response_model=list[schemas.Namespace])
def read_namespaces(
    *,
    db: Session = Depends(get_db),
    scopes: ScopeManager = Depends(get_scopes),
) -> list[models.Namespace]:
    return [
        namespace
        for namespace in crud.namespace.all(db=db)
        if scopes.can_read_in_namespace(namespace)
    ]


@router.get("/{path:path}", name="path-convertor", response_model=schemas.Namespace)
def read_namespace(
    *,
    path: str,
    db: Session = Depends(get_db),
    scopes: ScopeManager = Depends(get_scopes),
) -> models.Namespace:
    namespace = crud.namespace.get(db=db, path=path)

    if namespace is None or not scopes.can_read_in_namespace(namespace):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                "Namespace not found, or you do not have sufficient "
                "permissions to access this namespace."
            ),
        )
    return namespace


@router.post(
    "/",
    response_model=schemas.Namespace,
    status_code=HTTPStatus.CREATED,
    dependencies=[Depends(can_create_namespace)],
)
def create_namespace(
    *,
    loc_in: schemas.NamespaceCreate,
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
) -> models.Namespace:
    return crud.namespace.create(db=db, obj_in=loc_in, obj_meta=obj_meta)
