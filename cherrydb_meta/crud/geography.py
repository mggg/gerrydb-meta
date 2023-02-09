"""CRUD operations and transformations for geographic improts."""
import uuid
import logging

from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import CRBase
from cherrydb_meta.exceptions import CreateConflictError

log = logging.getLogger()


class CRGeography(CRBase[models.Geography, None]):
    def create_bulk(
        self,
        db: Session,
        *,
        objs_in: list[schemas.GeographyCreate],
        obj_meta: models.ObjectMeta,
        geo_import: models.GeoImport,
        namespace: models.Namespace,
    ) -> list[models.Geography]:
        """Creates a new geographic import."""
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
            if existing_geos:
                raise CreateConflictError(
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

        db.flush()
        return geos

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


geo_import = CRGeography(models.Geography)
