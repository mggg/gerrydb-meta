"""CRUD operations and transformations for location metadata."""
import logging
from typing import Collection

from sqlalchemy import exc
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import CRBase
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


def normalize_path(path: str) -> str:
    """Normalizes a path (removes leading, trailing, and duplicate slashes)."""
    return "/".join(seg for seg in path.lower().split("/") if seg)


class CRLocation(CRBase[models.Location, schemas.LocationCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.LocationCreate,
        obj_meta: models.ObjectMeta,
    ) -> models.Location:
        """Creates a new location with a canonical reference."""
        db.commit()
        with db.begin():
            # Look up the reference to a possible parent location.
            if obj_in.parent_path is not None:
                parent_ref = (
                    db.query(models.LocationRef)
                    .filter_by(path=obj_in.parent_path)
                    .first()
                )
                if parent_ref is None:
                    raise CreateValueError(
                        f"Reference to unknown parent location '{obj_in.parent_path}'."
                    )
                parent_id = parent_ref.loc_id
                if parent_id is None:
                    raise CreateValueError(
                        f"Parent location reference '{obj_in.parent_path}' does not point to a "
                        "valid location."
                    )
            else:
                parent_id = None

            # Create a path to the location.
            canonical_path = (normalize_path(obj_in.canonical_path),)
            canonical_ref = models.LocationRef(
                path=canonical_path, meta_id=obj_meta.meta_id
            )
            db.add(canonical_ref)
            try:
                db.flush()
            except exc.SQLAlchemyError:
                # TODO: Make this more specific--the primary goal is to capture the case
                # where the reference already exists.
                log.exception(
                    "Failed to create reference '%s' to new location.",
                    obj_in.canonical_path,
                )
                raise CreateValueError(
                    f"Failed to create canonical path '{canonical_path}' to new location. "
                    "(The path may already exist.)"
                )

            # Create the location itself.
            loc = models.Location(
                canonical_ref_id=canonical_ref.ref_id,
                parent_id=parent_id,
                meta_id=obj_meta.meta_id,
                name=obj_in.name,
            )
            db.add(loc)
            try:
                db.flush()
            except exc.SQLAlchemyError:
                log.exception("Failed to create new location.")
                raise CreateValueError("Failed to create new location.")

            canonical_ref.loc_id = loc.loc_id
            db.flush()

            # Create additional aliases (non-canonical references) to the location.
            if obj_in.aliases:
                self._add_aliases(
                    db=db, alias_paths=obj_in.aliases, loc=loc, obj_meta=obj_meta
                )

        return loc

    def get_by_ref(self, db: Session, *, path: str) -> models.Location | None:
        """Retrieves a location by reference path."""
        ref = (
            db.query(models.LocationRef)
            .filter(models.LocationRef.path == normalize_path(path))
            .first()
        )
        return None if ref is None else ref.loc

    def patch(
        self,
        db: Session,
        *,
        obj: models.Location,
        obj_meta: models.ObjectMeta,
        patch: schemas.LocationPatch,
    ) -> models.Location | None:
        """Patches a location (adds new aliases)."""
        refs = (
            db.query(models.LocationRef)
            .filter(models.LocationRef.loc_id == obj.loc_id)
            .all()
        )
        new_aliases = set(normalize_path(path) for path in patch.aliases) - set(
            ref.path for ref in refs
        )
        if not new_aliases:
            return obj

        db.commit()
        with db.begin():
            self._add_aliases(
                db=db, alias_paths=new_aliases, loc=obj, obj_meta=obj_meta
            )
        return obj

    def _add_aliases(
        self,
        *,
        db: Session,
        alias_paths: Collection[str],
        loc: models.Location,
        obj_meta: models.ObjectMeta,
    ) -> None:
        """Adds aliases to a location."""
        for alias_path in alias_paths:
            alias_ref = models.LocationRef(
                path=normalize_path(alias_path),
                loc_id=loc.loc_id,
                meta_id=obj_meta.meta_id,
            )
            db.add(alias_ref)

            try:
                db.flush()
            except exc.SQLAlchemyError:
                # TODO: Make this more specific--the primary goal is to capture the case
                # where the reference already exists.
                log.exception(
                    "Failed to create aliases for new location.",
                    loc.canonical_path,
                )
                raise CreateValueError(
                    f"Failed to create aliases for new location."
                    "(One or more aliases may already exist.)"
                )


location = CRLocation(models.Location)
