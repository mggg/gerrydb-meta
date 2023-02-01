"""Endpoints for locality metadata."""
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.deps import (
    get_db,
    get_obj_meta,
    can_read_localities,
    can_write_localities,
)

router = APIRouter()


@router.get(
    "/",
    response_model=list[schemas.Locality],
    dependencies=[Depends(can_read_localities)],
)
def read_localities(
    *,
    db: Session = Depends(get_db),
) -> list[models.Locality]:
    return crud.locality.all(db=db)


@router.get(
    "/{path:path}",
    response_model=schemas.Locality,
    dependencies=[Depends(can_read_localities)],
)
def read_locality(
    *,
    path: str,
    request: Request,
    db: Session = Depends(get_db),
) -> models.Locality:
    loc = crud.locality.get_by_ref(db=db, path=path)
    if loc is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="No locality found."
        )

    # Redirect to the canonical resource if an alias is used.
    canonical_path = loc.canonical_ref.path
    if canonical_path != path:
        return RedirectResponse(
            url=str(request.url).replace(path, canonical_path),
            status_code=HTTPStatus.PERMANENT_REDIRECT,
        )
    return loc


@router.patch(
    "/{path:path}",
    response_model=schemas.Locality,
    dependencies=[Depends(can_write_localities)],
)
def patch_locality_aliases(
    *,
    path: str,
    loc_patch: schemas.LocalityPatch,
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
) -> models.Locality:
    loc = crud.locality.get_by_ref(db=db, path=path)
    if loc is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="No locality found."
        )
    return crud.locality.patch(db=db, obj=loc, obj_meta=obj_meta, patch=loc_patch)


@router.post(
    "/",
    response_model=schemas.Locality,
    status_code=HTTPStatus.CREATED,
    dependencies=[Depends(can_write_localities)],
)
def create_locality(
    *,
    loc_in: schemas.LocalityCreate,
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
) -> models.Locality:
    return crud.locality.create(db=db, obj_in=loc_in, obj_meta=obj_meta)
