"""Endpoints for location metadata."""
from http import HTTPStatus
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.deps import get_db, get_obj_meta

router = APIRouter()


@router.get("/", response_model=list[schemas.Location])
def read_locations(
    *,
    db: Session = Depends(get_db),
) -> list[models.Location]:
    return crud.location.all(db=db)


@router.get("/{path:path}", name="path-convertor", response_model=schemas.Location)
def read_location(
    *,
    request: Request,
    db: Session = Depends(get_db),
    path: str,
) -> models.Location:
    loc = crud.location.get_by_ref(db=db, path=path)
    if loc is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="No location found."
        )

    # Redirect to the canonical resource if an alias is used.
    canonical_path = loc.canonical_ref.path
    if canonical_path != path:
        return RedirectResponse(
            url=str(request.url).replace(path, canonical_path),
            status_code=HTTPStatus.PERMANENT_REDIRECT,
        )
    return loc


@router.post("/", response_model=schemas.Location)
def create_location(
    *,
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
    loc_in: schemas.LocationCreate,
) -> models.Location:
    return crud.location.create(db=db, obj_in=loc_in, obj_meta=obj_meta)
