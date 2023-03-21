"""CRUD operations and transformations for districting plans."""
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Tuple

from sqlalchemy import exc, func, label, or_, select, union
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase, normalize_path
from cherrydb_meta.crud.column import COLUMN_TYPE_TO_VALUE_COLUMN
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()


class CRGraph(NamespacedCRBase[models.Graph, schemas.GraphCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.GraphCreate,
        geo_set_version: models.GeoSetVersion,
        edge_geos: dict[str, models.Geography],
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
    ) -> Tuple[models.Plan, uuid.UUID]:
        """Creates a new graph."""
        set_geos = [member.geo for member in geo_set_version.members]
        not_in_geo_set = set(geo.geo_id for geo in edge_geos) - set(
            geo.geo_id for geo in set_geos
        )
        if not_in_geo_set:
            bad_geo_paths = [
                geo.full_path for geo in edge_geos if geo.geo_id in not_in_geo_set
            ]
            raise CreateValueError(
                "Geographies not associated with locality and layer: "
                f"{', '.join(bad_geo_paths)}"
            )

        with db.begin(nested=True):
            graph = models.Graph(
                set_version_id=geo_set_version.set_version_id,
                namespace_id=namespace.namespace_id,
                path=normalize_path(obj_in.path),
                description=obj_in.description,
                meta_id=obj_meta.meta_id,
            )

            try:
                db.flush()
            except exc.SQLAlchemyError:
                # TODO: Make this more specific--the primary goal is to capture the case
                # where the reference already exists.
                log.exception("Failed to create new graph.")
                raise CreateValueError(
                    "Failed to create new graph. (The path(s) may already exist.)"
                )

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.View | None:
        """Retrieves a graph by path.

        Args:
            path: Path to graph (namespace excluded).
            namespace: Graph's namespace.
        """
        return (
            db.query(models.Graph)
            .filter(
                models.Graph.namespace_id == namespace.namespace_id,
                models.Graph.path == normalize_path(path),
            )
            .first()
        )


graph = CRGraph(models.Graph)
