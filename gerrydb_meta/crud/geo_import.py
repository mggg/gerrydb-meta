"""CRUD operations and transformations for geographic improts."""
import logging
import uuid
from typing import Tuple

from sqlalchemy.orm import Session

from gerrydb_meta import models, schemas
from gerrydb_meta.crud.base import NamespacedCRBase

log = logging.getLogger()


class CRGeoImport(NamespacedCRBase[models.GeoImport, None]):
    def create(
        self,
        db: Session,
        *,
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> Tuple[models.GeoImport, uuid.UUID]:
        """Creates a new geographic import."""
        with db.begin(nested=True):
            geo_import = models.GeoImport(
                namespace_id=namespace.namespace_id,
                meta_id=obj_meta.meta_id,
                created_by=obj_meta.created_by,
            )
            db.add(geo_import)
            etag = self._update_etag(db, namespace)

        db.flush()
        db.refresh(geo_import)
        return geo_import, etag

    def get(self, db: Session, *, uuid: uuid.UUID) -> models.GeoImport | None:
        """Retrieves a geographic import by UUID.

        Args:
            uuid: UUID of geographic import (namespace excluded).
            namespace: Geographic layer's namespace.
        """
        return db.query(models.GeoImport).filter(models.GeoImport.uuid == uuid).first()


geo_import = CRGeoImport(models.GeoImport)
