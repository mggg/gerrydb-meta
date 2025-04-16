"""CRUD operations and transformations for geographic layers."""

import logging
import uuid
from datetime import datetime, timezone
from typing import Tuple

from sqlalchemy import exc, insert, update
from sqlalchemy.orm import Session

from gerrydb_meta import models, schemas
from gerrydb_meta.crud.base import NamespacedCRBase, normalize_path
from gerrydb_meta.exceptions import CreateValueError
from uvicorn.config import logger as log


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
        log.debug("TOP OF CREATE GEO LAYER")

        with db.begin(nested=True):
            # Create a path to the column.
            canonical_path = normalize_path(obj_in.path)
            geo_layer = models.GeoLayer(
                path=canonical_path,
                description=obj_in.description,
                namespace_id=namespace.namespace_id,
                meta_id=obj_meta.meta_id,
                source_url=obj_in.source_url,
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
        now = datetime.now(timezone.utc)

        if len(set(geo.namespace_id for geo in geographies)) > 1:
            raise CreateValueError(
                "Cannot map geographies in multiple namespaces "
                "to a geographic layer."
            )

        new_geo_ids = set(geo.geo_id for geo in geographies)

        log.debug("NEW GEO IDS: %s", new_geo_ids)

        with db.begin(nested=True):
            # First check to see if we need to create a new geo set
            old_geo_ids = set(
                [
                    item[0]
                    for item in db.query(models.GeoSetMember.geo_id)
                    .join(
                        models.GeoSetVersion,
                        models.GeoSetMember.set_version_id
                        == models.GeoSetVersion.set_version_id,
                    )
                    .all()
                ]
            )

            log.debug("OLD GEO IDS: %s", old_geo_ids)

            if old_geo_ids == new_geo_ids:
                # No need to create a new set
                log.debug(
                    f"Attempted to create a new geo set for layer {layer.full_path}"
                    f" in the namespace {layer.namespace.path} at locality "
                    f" {locality.canonical_ref} but the new set is identical"
                    f" to the old set."
                )
                db.flush()
                return

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

    def get_set_by_locality(
        self,
        db: Session,
        *,
        layer: models.GeoLayer,
        locality: models.Locality,
    ) -> models.GeoSetVersion | None:
        """Retrieves the latest `GeoSetVersion` associated with a locality."""
        return (
            db.query(models.GeoSetVersion)
            .filter(
                models.GeoSetVersion.valid_to.is_(None),
                models.GeoSetVersion.layer_id == layer.layer_id,
                models.GeoSetVersion.loc_id == locality.loc_id,
            )
            .first()
        )


geo_layer = CRGeoLayer(models.GeoLayer)
