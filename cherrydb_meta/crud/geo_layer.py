"""CRUD operations and transformations for geographic layers."""
import logging
import uuid
from datetime import datetime
from typing import Tuple

from sqlalchemy import exc, insert, update
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase, normalize_path
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


class CRGeoLayer(NamespacedCRBase[models.GeoLayer, schemas.GeoLayerCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.GeoLayerCreate,
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> Tuple[models.GeoLayer, uuid.UUID]:
        """Creates a new geographic layer."""
        with db.begin(nested=True):
            # Create a path to the column.
            canonical_path = normalize_path(obj_in.path)
            geo_layer = models.GeoLayer(
                path=canonical_path,
                description=obj_in.description,
                namespace_id=namespace.namespace_id,
                meta_id=obj_meta.meta_id,
            )
            db.add(geo_layer)

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
            etag = self._update_etag(db, namespace)

        db.refresh(geo_layer)
        return geo_layer, etag

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

    def map_locality(
        self,
        db: Session,
        *,
        layer: models.GeoLayer,
        locality: models.Locality,
        geographies: list[models.Geography],
        obj_meta: models.ObjectMeta,
    ) -> None:
        """Maps a set of `geographies` to `layer` in `locality`."""
        now = datetime.now()
        with db.begin(nested=True):
            # Deprecate old version if present.
            db.execute(
                update(models.GeoSetVersion)
                .where(
                    models.GeoSetVersion.layer_id == layer.layer_id,
                    models.GeoSetVersion.loc_id == locality.loc_id,
                    models.GeoSetVersion.valid_to.is_(None),
                )
                .values(valid_to=now)
            )

            set_version = models.GeoSetVersion(
                layer_id=layer.layer_id,
                loc_id=locality.loc_id,
                meta_id=obj_meta.meta_id,
                valid_from=now,
            )
            db.add(set_version)
            db.flush()
            db.refresh(set_version)

            db.execute(
                insert(models.GeoSetMember),
                [
                    {
                        "set_version_id": set_version.set_version_id,
                        "geo_id": geo.geo_id,
                    }
                    for geo in geographies
                ],
            )


geo_layer = CRGeoLayer(models.GeoLayer)
