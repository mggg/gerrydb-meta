"""CRUD operations and transformations for geographic imports."""
import logging
import uuid
from typing import Tuple

from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase
from cherrydb_meta.exceptions import BulkCreateError

log = logging.getLogger()


class CRGeography(NamespacedCRBase[models.Geography, None]):
    def create_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyCreate],
        obj_meta: models.ObjectMeta,
        geo_import: models.GeoImport,
        namespace: models.Namespace,
    ) -> Tuple[list[models.Geography], uuid.UUID]:
        """Creates new geographies, possibly in bulk."""
        with db.begin(nested=True):
            # TODO: check for existing, raise error.
            existing_geos = (
                db.query(models.Geography.path)
                .filter(
                    models.Geography.path.in_(obj_in.path for obj_in in objs_in),
                    models.Geography.namespace_id == namespace.namespace_id,
                )
                .all()
            )

            geos = [
                models.Geography(
                    path=obj_in.path,
                    meta_id=obj_meta.meta_id,
                    namespace_id=namespace.namespace_id,
                )
                for obj_in in objs_in
            ]
            db.flush()

            for geo, obj_in in zip(geos, objs_in):
                db.add(
                    models.GeoInstance(
                        import_id=geo_import.import_id,
                        geo_id=geo.geo_id,
                        geometry=obj_in.geometry,
                    )
                )

            etag = self._update_etag(db, namespace)

        db.flush()
        if existing_geos:
            raise BulkCreateError(
                "Cannot create geographies that already exist.",
                paths=[geo.path for geo in existing_geos],
            )
        return geos, etag

    def patch_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyCreate],
        obj_meta: models.ObjectMeta,
        geo_import: models.GeoImport,
        namespace: models.Namespace,
    ) -> Tuple[list[models.Geography], uuid.UUID]:
        """Creates a new geographic import."""
        with db.begin(nested=True):
            existing_geos = (
                db.query(models.Geography.path)
                .filter(
                    models.Geography.path.in_(obj_in.path for obj_in in objs_in),
                    models.Geography.namespace_id == namespace.namespace_id,
                )
                .all()
            )
            if existing_geos:
                raise BulkCreateError(
                    "Cannot create geographies that already exist.",
                    paths=[geo.path for geo in existing_geos],
                )

            geos = [
                models.Geography(
                    path=obj_in.path,
                    meta_id=obj_meta.meta_id,
                    namespace_id=namespace.namespace_id,
                )
                for obj_in in objs_in
            ]
            db.flush()

            for geo, obj_in in zip(geos, objs_in):
                db.add(
                    models.GeoInstance(
                        import_id=geo_import.import_id,
                        geo_id=geo.geo_id,
                        geometry=obj_in.geometry,
                    )
                )
            etag = self._update_etag(db, namespace)

        db.flush()
        return geos, etag

    def get(
        self, db: Session, *, uuid: uuid.UUID, namespace: models.Namespace
    ) -> models.Geography | None:
        """Retrieves a geographic import by UUID.

        Args:
            uuid: UUID of geographic import (namespace excluded).
            namespace: Geographic layer's namespace.
        """
        return (
            db.query(models.Geography)
            .filter(
                models.Geography.namespace_id == namespace.namespace_id,
                models.Geography.uuid == uuid,
            )
            .first()
        )


geography = CRGeography(models.Geography)
