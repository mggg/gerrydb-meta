"""CRUD operations and transformations for geographic improts."""
import uuid
import logging

from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import CRBase

log = logging.getLogger()


class CRGeoImport(CRBase[models.GeoImport, None]):
    def create(
        self,
        db: Session,
        *,
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> models.GeoImport:
        """Creates a new geographic import."""
        with db.begin(nested=True):
            geo_import = models.GeoImport(
                namespace_id=namespace.namespace_id,
                meta_id=obj_meta.meta_id,
            )

        db.flush()
        db.refresh(geo_import)
        return geo_import

    def get(
        self, db: Session, *, uuid: uuid.UUID, namespace: models.Namespace
    ) -> models.GeoImport | None:
        """Retrieves a geographic import by UUID.

        Args:
            uuid: UUID of geographic import (namespace excluded).
            namespace: Geographic layer's namespace.
        """
        return (
            db.query(models.GeoImport)
            .filter(
                models.GeoImport.namespace_id == namespace.namespace_id,
                models.GeoImport.uuid == uuid,
            )
            .first()
        )


geo_import = CRGeoImport(models.GeoImport)
