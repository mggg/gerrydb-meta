"""CRUD operations and transformations for view templates."""
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
        models.ViewTemplateVersion.template_version_id == template_version_id
    )
    column_set_ids = select(models.ViewTemplateColumnSetMember.set_id).filter(
        models.ViewTemplateVersion.template_version_id == template_version_id
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
                bad_cols.append((column.full_path, value_count))

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
                    f"Failed to create view '{canonical_path}'."
                    "(The path may already exist in the namespace.)"
                )

            etag = self._update_etag(db, namespace)

        db.refresh(view)
        return view, etag

    # TODO: patch()

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

    def instantiate(
        self, db: Session, *, view: models.View
    ) -> tuple[list[models.GeoVersion], dict[str, list]]:
        """Retrieves data associated with a view.

        Returns:
            A 2-tuple containing:
                (1) A list of versioned geographies associated with the view.
                (2) TODO...
        """
        set_version_id = _geo_set_version_id(db, view.loc, view.layer, view.at)
        # TODO: bespoke exceptions?
        if set_version_id is None:
            raise ValueError("Invalid view: versioned geographies not found.")

        columns = _view_columns(db, view.template_version_id)
        geo_set_members = select(models.GeoSetMember.geo_id).filter(
            models.GeoSetMember.set_version_id == set_version_id
        )
        geo_versions = (
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
            .all()
        )

        # Convert column values into the form
        # {<column path>: <values in order of `geo_versions`>}.
        col_paths = {col.col_id: col.canonical_ref.full_path for col in columns}
        col_types = {col.col_id: col.type for col in columns}
        col_values_raw = (
            db.query(models.ColumnValue)
            .filter(
                models.ColumnValue.geo_id.in_(geo_set_members),
                models.ColumnValue.col_id.in_(col.col_id for col in columns),
                models.ColumnValue.valid_from <= view.at,
                or_(
                    models.ColumnValue.valid_to.is_(None),
                    models.ColumnValue.valid_to >= view.at,
                ),
            )
            .all()
        )

        col_values_mapped = defaultdict(dict)
        for value in col_values_raw:
            value_col = COLUMN_TYPE_TO_VALUE_COLUMN[col_types[value.col_id]]
            col_values_mapped[value.col_id][value.geo_id] = getattr(value, value_col)

        col_values = {
            col_paths[col_id]: [v for _, v in sorted(vals_by_geo.items())]
            for col_id, vals_by_geo in col_values_mapped.items()
        }
        return geo_versions, col_values


view = CRView(models.View)
