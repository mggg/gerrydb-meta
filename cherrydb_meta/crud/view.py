"""CRUD operations and transformations for views."""
import logging
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Tuple

from sqlalchemy import exc, func, label, or_, select, union
from sqlalchemy.orm import Query, Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase, normalize_path
from cherrydb_meta.crud.column import COLUMN_TYPE_TO_VALUE_COLUMN
from cherrydb_meta.exceptions import CreateValueError

log = logging.getLogger()

# Server-side cursor configuration.
GEO_BATCH_SIZE = 5000
COLUMN_VALUE_BATCH_SIZE = 50000


def _geo_set_version_id(
    db: Session, locality: models.Locality, layer: models.GeoLayer, at: datetime
) -> int | None:
    """Gets the primary key of a GeoSetVersion by (locality, layer, at)."""
    return (
        db.query(models.GeoSetVersion.set_version_id)
        .filter(
            models.GeoSetVersion.loc_id == locality.loc_id,
            models.GeoSetVersion.layer_id == layer.layer_id,
            models.GeoSetVersion.valid_from <= at,
            or_(
                models.GeoSetVersion.valid_to.is_(None),
                models.GeoSetVersion.valid_to >= at,
            ),
        )
        .scalar()
    )


def _view_columns(db: Session, template_version_id: int) -> list[models.DataColumn]:
    """Gets the unique columns associated with a `ViewTemplateVersion`."""
    column_ref_ids = select(models.ViewTemplateColumnMember.ref_id).filter(
        models.ViewTemplateColumnMember.template_version_id == template_version_id
    )
    column_set_ids = select(models.ViewTemplateColumnSetMember.set_id).filter(
        models.ViewTemplateColumnSetMember.template_version_id == template_version_id
    )
    column_set_ref_ids = select(models.ColumnSetMember.ref_id).filter(
        models.ColumnSetMember.set_id.in_(column_set_ids)
    )
    column_ids = select(models.ColumnRef.col_id).filter(
        models.ColumnRef.ref_id.in_(union(column_set_ref_ids, column_ref_ids))
    )
    return (
        db.query(models.DataColumn)
        .filter(models.DataColumn.col_id.in_(column_ids))
        .all()
    )


@dataclass(frozen=True)
class ViewStream:
    """Iterables for instantiated view data."""

    geo_count: int
    geographies: Iterator
    column_values: dict[str, Iterator]
    plans: list[models.Plan]


class CRView(NamespacedCRBase[models.View, schemas.ViewCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.ViewCreate,
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
        template: models.ViewTemplate,
        locality: models.Locality,
        layer: models.GeoLayer,
        graph: models.Graph | None = None,
    ) -> Tuple[models.View, uuid.UUID]:
        """Creates a new view."""
        valid_at = (
            datetime.now(timezone.utc) if obj_in.valid_at is None else obj_in.valid_at
        )
        if valid_at > datetime.now(timezone.utc):
            raise CreateValueError("Cannot instantiate view in the future.")

        # Verify that the view can be instantiated:
        #   * locality + layer + valid_at identify a GeoSet.
        #   * For all geographies in the GeoSet, and for all columns in `template`,
        #     column values exist at `valid_at`.
        set_version_id = _geo_set_version_id(db, locality, layer, valid_at)
        if set_version_id is None:
            raise CreateValueError(
                "Cannot instantiate view: no set of geographies exists "
                "satisfying locality, layer, and time constraints."
            )

        if graph is not None and graph.set_version_id != set_version_id:
            raise CreateValueError(
                f'Cannot instantiate view: graph "{graph.full_path}" does not match '
                f'locality "{locality.canonical_ref.path}" and geographic layer '
                f'"{layer.full_path}".'
            )
        if graph is not None and graph.created_at > valid_at:
            raise CreateValueError(
                f'Cannot instantiate view: graph "{graph.full_path}" exists '
                f"in the future relative to view timestamp ({valid_at})."
            )

        template_version_id = (
            db.query(models.ViewTemplateVersion.template_version_id)
            .filter(
                models.ViewTemplateVersion.template_id == template.template_id,
                models.ViewTemplateVersion.valid_from <= valid_at,
                or_(
                    models.ViewTemplateVersion.valid_to.is_(None),
                    models.ViewTemplateVersion.valid_to >= valid_at,
                ),
            )
            .scalar()
        )
        if template_version_id is None:
            raise CreateValueError(
                "No template version found satisfying time constraints."
            )

        columns = _view_columns(db, template_version_id)
        geo_set_members = (
            db.query(models.GeoSetMember.geo_id)
            .filter(models.GeoSetMember.set_version_id == set_version_id)
            .all()
        )
        num_geos = len(geo_set_members)
        value_counts = (
            db.query(
                models.ColumnValue.col_id,
                label("geo_count", func.count(models.ColumnValue.geo_id)),
            )
            .filter(
                models.ColumnValue.geo_id.in_(
                    member.geo_id for member in geo_set_members
                ),
                models.ColumnValue.col_id.in_(col.col_id for col in columns),
                models.ColumnValue.valid_from <= valid_at,
                (
                    (models.ColumnValue.valid_to.is_(None))
                    | (models.ColumnValue.valid_to >= valid_at)
                ),
            )
            .group_by(models.ColumnValue.col_id)
            .all()
        )

        value_counts_by_col = {group.col_id: group.geo_count for group in value_counts}
        bad_cols = []
        for column in columns:
            value_count = value_counts_by_col.get(column.col_id, 0)
            if value_count < num_geos:
                bad_cols.append((column.canonical_ref.full_path, value_count))

        if bad_cols:
            bad_cols_formatted = ", ".join(
                f"{col_path} ({count} values found, {num_geos} values expected)"
                for col_path, count in bad_cols
            )
            raise CreateValueError(
                "Cannot instantiate view: column values satisfying time "
                "constraints not available for all geographies. Bad columns: "
                + bad_cols_formatted
            )

        canonical_path = normalize_path(obj_in.path)
        with db.begin(nested=True):
            view = models.View(
                path=canonical_path,
                namespace_id=namespace.namespace_id,
                meta_id=obj_meta.meta_id,
                template_id=template.template_id,
                template_version_id=template_version_id,
                loc_id=locality.loc_id,
                layer_id=layer.layer_id,
                graph_id=None if graph is None else graph.graph_id,
                at=valid_at,
                proj=obj_in.proj,
            )
            db.add(view)

            try:
                db.flush()
            except exc.SQLAlchemyError:
                log.exception(
                    "Failed to create view '%s'.",
                    canonical_path,
                )
                raise CreateValueError(
                    f"Failed to create view '{canonical_path}'. "
                    "(The path may already exist in the namespace.)"
                )

            etag = self._update_etag(db, namespace)

        db.refresh(view)
        return view, etag

    def get(
        self, db: Session, *, path: str, namespace: models.Namespace
    ) -> models.View | None:
        """Retrieves a view by reference path.

        Args:
            path: Path to view (namespace excluded).
            namespace: View's namespace.
        """
        return (
            db.query(models.View)
            .filter(
                models.View.namespace_id == namespace.namespace_id,
                models.View.path == normalize_path(path),
            )
            .first()
        )

    def instantiate(self, db: Session, *, view: models.View) -> ViewStream:
        """Retrieves iterators for data associated with a view."""
        set_version_id = _geo_set_version_id(db, view.loc, view.layer, view.at)
        if set_version_id is None:
            raise ValueError("Invalid view: versioned geographies not found.")

        columns = _view_columns(db, view.template_version_id)

        geo_set_members = select(models.GeoSetMember.geo_id).filter(
            models.GeoSetMember.set_version_id == set_version_id
        )
        geo_count = db.execute(
            select(func.count(models.GeoSetMember.geo_id)).filter(
                models.GeoSetMember.set_version_id == set_version_id
            )
        ).first()[0]
        geo_query = (
            db.scalars(
                db.query(models.GeoVersion)
                .filter(
                    models.GeoVersion.geo_id.in_(geo_set_members),
                    models.GeoVersion.valid_from <= view.at,
                    (
                        (models.GeoVersion.valid_to.is_(None))
                        | (models.GeoVersion.valid_to <= view.at)
                    ),
                )
                .order_by(models.GeoVersion.geo_id)
            )
            .yield_per(GEO_BATCH_SIZE)
            .partitions()
        )

        # Create generators for each column.
        col_queries = {}
        for col in columns:
            value_col = COLUMN_TYPE_TO_VALUE_COLUMN[col.type]
            col_queries[col.canonical_ref.full_path] = (
                db.scalars(
                    db.query(getattr(models.ColumnValue, value_col).label("val"))
                    .filter(
                        models.ColumnValue.geo_id.in_(geo_set_members),
                        models.ColumnValue.col_id == col.col_id,
                        models.ColumnValue.valid_from <= view.at,
                        or_(
                            models.ColumnValue.valid_to.is_(None),
                            models.ColumnValue.valid_to >= view.at,
                        ),
                    )
                    .order_by(models.ColumnValue.geo_id)
                )
                .yield_per(COLUMN_VALUE_BATCH_SIZE)
                .partitions()
            )

        # Find all plans compatible with the `GeoSetVersion` that existed
        # when the view was created.
        plans = (
            db.query(models.Plan)
            .filter(
                models.Plan.set_version_id == set_version_id,
                models.Plan.created_at <= view.at,
            )
            .all()
        )
        # Apply the public join constraint: don't leak any private plans.
        visible_plans = [
            plan
            for plan in plans
            if (
                plan.namespace.public
                or plan.namespace.namespace_id == view.namespace.namespace_id
            )
        ]
        return ViewStream(
            geo_count=geo_count,
            geographies=geo_query,
            column_values=col_queries,
            plans=visible_plans,
        )


view = CRView(models.View)
