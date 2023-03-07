"""CRUD operations and transformations for location metadata."""
import logging
import uuid
from typing import Collection, Tuple

from sqlalchemy import exc
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import CRBase, normalize_path
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


class CRLocality(CRBase[models.Locality, schemas.LocalityCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.LocalityCreate,
        obj_meta: models.ObjectMeta,
    ) -> Tuple[models.Locality, uuid.UUID]:
        """Creates a new location with a canonical reference."""
        with db.begin(nested=True):
            # Look up the reference to a possible parent location.
            if obj_in.parent_path is not None:
                parent_ref = (
                    db.query(models.LocalityRef)
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
            canonical_path = normalize_path(obj_in.canonical_path)
            canonical_ref = models.LocalityRef(
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
            loc = models.Locality(
                canonical_ref_id=canonical_ref.ref_id,
                parent_id=parent_id,
                meta_id=obj_meta.meta_id,
                name=obj_in.name,
                default_proj=obj_in.default_proj,
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

            etag = self._update_etag(db)

        return loc, etag

    def get_by_ref(self, db: Session, *, path: str) -> models.Locality | None:
        """Retrieves a location by reference path."""
        ref = (
            db.query(models.LocalityRef)
            .filter(models.LocalityRef.path == normalize_path(path))
            .first()
        )
        return None if ref is None else ref.loc

    def patch(
        self,
        db: Session,
        *,
        obj: models.Locality,
        obj_meta: models.ObjectMeta,
        patch: schemas.LocalityPatch,
    ) -> Tuple[models.Locality, uuid.UUID]:
        """Patches a location (adds new aliases)."""
        new_aliases = set(normalize_path(path) for path in patch.aliases) - set(
            ref.path for ref in obj.refs
        )
        if not new_aliases:
            return obj

        etag = self._update_etag(db)
        db.flush()
        self._add_aliases(db=db, alias_paths=new_aliases, loc=obj, obj_meta=obj_meta)
        db.refresh(obj)
        return obj, etag

    def _add_aliases(
        self,
        *,
        db: Session,
        alias_paths: Collection[str],
        loc: models.Locality,
        obj_meta: models.ObjectMeta,
    ) -> None:
        """Adds aliases to a location."""
        for alias_path in alias_paths:
            alias_ref = models.LocalityRef(
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
                    "Failed to create aliases for new location."
                    "(One or more aliases may already exist.)"
                )


locality = CRLocality(models.Locality)
