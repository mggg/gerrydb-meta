"""Endpoints for namespace metadata."""
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.deps import get_db, get_obj_meta

router = APIRouter()


@router.get("/", response_model=list[schemas.Namespace])
def read_namespaces(
    *,
    db: Session = Depends(get_db),
) -> list[models.Namespace]:
    return crud.namespace.all(db=db)


@router.get("/{path:path}", name="path-convertor", response_model=schemas.Namespace)
def read_namespace(
    *,
    path: str,
    db: Session = Depends(get_db),
) -> models.Namespace:
    namespace = crud.namespace.get(db=db, path=path)
    if namespace is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="No namespace found."
        )


@router.post("/", response_model=schemas.Namespace, status_code=HTTPStatus.CREATED)
def create_namespace(
    *,
    loc_in: schemas.NamespaceCreate,
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
) -> models.Namespace:
    return crud.namespace.create(db=db, obj_in=loc_in, obj_meta=obj_meta)
