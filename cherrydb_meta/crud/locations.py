"""CRUD operations and transformations for location metadata."""
import logging
from sqlalchemy import exc
from sqlalchemy.orm import Session
from cherrydb_meta.crud.base import CRBase
from cherrydb_meta.exceptions import CreateValueError
from cherrydb_meta import models, schemas 

log = logging.getLogger()

class CRLocation(CRBase[models.Location, schemas.LocationCreate]):
    def create(self, db: Session, *, obj_in: schemas.LocationCreate, obj_meta: models.ObjectMeta) -> models.Location:
        # Look up the reference to a possible parent location.
        if obj_in.parent_path is not None:
            parent_ref = db.query(models.LocationRef).filter_by(path=obj_in.parent_path).first()
            if parent_ref is None:
                raise CreateValueError(f"Reference to unknown parent location '{obj_in.parent_path}'.")
            parent_id = parent_ref.loc_id
            if parent_id is None:
                raise CreateValueError(
                    f"Parent location reference '{obj_in.parent_path}' does not point to a "
                    "valid location."
                )
        else:
            parent_id = None
           
        canonical_ref = models.LocationRef(
            path=obj_in.canonical_path,
            meta_id=obj_meta.meta_id
        )
        db.add(canonical_ref)
        try:
            db.commit()
        except exc.SQLAlchemyError:
            # TODO: Make this more specific--the primary goal is to capture the case
            # where the reference already exists.
            log.exception(
                "Failed to create reference '%s' to new location.", obj_in.canonical_path
            )
            raise CreateValueError(
                f"Failed to create reference '{obj_in.canonical_path}' to new location. "
                "(The reference may already exist.)"
            )
            
        loc = models.Location(
            canonical_ref_id=canonical_ref.ref_id,
            parent_id=parent_id,
            meta_id=obj_meta.meta_id,
            name=obj_in.name
        )
        db.add(loc)
        try:
            db.commit()
        except exc.SQLAlchemyError:
            log.exception("Failed to create new location.")
            raise CreateValueError("Failed to create new location.")
    
        canonical_ref.loc_id = loc.loc_id
        db.commit()
        return loc
    
    

location = CRLocation(models.Location)