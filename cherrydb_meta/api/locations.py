"""Endpoints for location metadata."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from cherrydb_meta import crud, models, schemas
from cherrydb_meta.api.deps import get_db, get_obj_meta

router = APIRouter()

@router.get("/", response_model=list[schemas.Location])
def read_locations() -> list[models.Location]:
   pass 


@router.post("/", response_model=schemas.Location)
def create_location(
    *,
    db: Session = Depends(get_db),
    obj_meta: models.ObjectMeta = Depends(get_obj_meta),
    loc_in: schemas.LocationCreate
) -> models.Location:
    return crud.location.create(db=db, obj_in=loc_in, obj_meta=obj_meta)