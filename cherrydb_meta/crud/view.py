"""CRUD operations and transformations for views."""
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Tuple

from sqlalchemy import exc, func, label, or_, select, union
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from cherrydb_meta import models, schemas
from cherrydb_meta.crud.base import NamespacedCRBase, normalize_path
from cherrydb_meta.crud.column import COLUMN_TYPE_TO_VALUE_COLUMN
from cherrydb_meta.exceptions import CreateValueError

_ST_ASBINARY_REGEX = re.compile(r"ST\_AsBinary\(([a-zA-Z0-9_.]+)\)")

log = logging.getLogger()


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
class ViewRenderContext:
    """Context for rendering a view's data and metadata."""

    query: str


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
                set_version_id=set_version_id,
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

    def render(self, db: Session, *, view: models.View) -> ViewRenderContext:
        """Generates a query to retrieve tabular view data.

        Used for bulk exports via `ogr2ogr`.
        """
        columns = _view_columns(db, view.template_version_id)
        members_sub = (
            select(
                models.GeoSetMember.set_version_id,
                models.GeoSetMember.geo_id,
            )
            .filter(models.GeoSetMember.set_version_id == view.set_version_id)
            .subquery()
        )
        geo_sub = select(models.Geography.geo_id, models.Geography.path).subquery()

        # Generate subqueries for joining tabular data.
        column_subs = []
        column_labels = []
        for col in columns:
            value_col = COLUMN_TYPE_TO_VALUE_COLUMN[col.type]
            # TODO: use preferred path.
            # TODO: make sure that labels are always valid and unique (slashes, etc.?)
            column_sub = (
                select(
                    models.ColumnValue.geo_id,
                    getattr(models.ColumnValue, value_col).label(
                        col.canonical_ref.path
                    ),
                )
                .filter(
                    models.ColumnValue.col_id == col.col_id,
                    models.ColumnValue.valid_from <= view.at,
                    or_(
                        models.ColumnValue.valid_to.is_(None),
                        models.ColumnValue.valid_to >= view.at,
                    ),
                )
                .subquery()
            )
            column_subs.append(column_sub)
            column_labels.append(column_sub.c[col.canonical_ref.path])

        query = (
            select(
                geo_sub.c.path,
                models.GeoVersion.geography,
                # TODO: internal points
                *column_labels,
            )
            .join(
                members_sub,
                members_sub.c.geo_id == models.GeoVersion.geo_id,
            )
            .join(geo_sub, geo_sub.c.geo_id == models.GeoVersion.geo_id)
        )

        for column_sub in column_subs:
            query = query.join(
                column_sub, column_sub.c.geo_id == models.GeoVersion.geo_id
            )

        query = query.where(
            models.GeoVersion.valid_from <= view.at,
            or_(
                models.GeoVersion.valid_to.is_(None),
                models.GeoVersion.valid_to >= view.at,
            ),
        )

        # Query generation: substitute in literals and remove the
        # ST_AsBinary() calls added by GeoAlchemy2.
        return ViewRenderContext(
            query=re.sub(
                _ST_ASBINARY_REGEX,
                r"\1",
                str(
                    query.compile(
                        dialect=postgresql.dialect(),
                        compile_kwargs={"literal_binds": True},
                    )
                ),
            )
        )


view = CRView(models.View)
