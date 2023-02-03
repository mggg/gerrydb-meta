"""CRUD operations and transformations for geographic layers."""
import logging

from sqlalchemy import exc
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import CRBase, normalize_path
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


class CRGeoLayer(CRBase[models.GeoLayer, schemas.GeoLayerCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.GeoLayerCreate,
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> models.GeoLayer:
        """Creates a new geographic layer."""
        with db.begin(nested=True):
            # Create a path to the column.
            canonical_path = normalize_path(obj_in.canonical_path)
            column_set = models.GeoLayer(
                path=canonical_path,
                description=obj_in.description,
                namespace_id=namespace.namespace_id,
                meta_id=obj_meta.meta_id,
            )

            try:
                db.flush()
            except exc.SQLAlchemyError:
                log.exception(
                    "Failed to create geographic layer '%s'.",
                    canonical_path,
                )
                raise CreateValueError(
                    f"Failed to create geographic layer '{canonical_path}'."
                    "(The path may already exist in the namespace.)"
                )

        db.refresh(column_set)
        return column_set

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.GeoLayer | None:
        """Retrieves a geographic layer by reference path.

        Args:
            path: Path to geographic layer (namespace excluded).
            namespace: Geographic layer's namespace.
        """
        return (
            db.query(models.GeoLayer)
            .filter(
                models.GeoLayer.namespace_id == namespace.namespace_id,
                models.GeoLayer.path == normalize_path(path),
            )
            .first()
        )


geo_layer = CRGeoLayer(models.GeoLayer)
