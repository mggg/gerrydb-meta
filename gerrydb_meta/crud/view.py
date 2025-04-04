"""CRUD operations and transformations for views."""

import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

from geoalchemy2 import Geometry
from geoalchemy2 import func as geo_func
from sqlalchemy import Sequence, cast, exc, func, label, or_, select, union, bindparam
from sqlalchemy import Table, Column, Integer, literal_column
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session
from sqlalchemy.sql import text, column

from gerrydb_meta import models, schemas
from gerrydb_meta.crud.base import NamespacedCRBase, normalize_path
from gerrydb_meta.crud.column import COLUMN_TYPE_TO_VALUE_COLUMN
from gerrydb_meta.enums import ViewRenderStatus
from gerrydb_meta.exceptions import CreateValueError
from uvicorn.config import logger as log

_ST_ASBINARY_REGEX = re.compile(r"ST\_AsBinary\(([a-zA-Z0-9_.]+)\)")


PLAN_BATCH_SIZE = 10000
GRAPH_BATCH_SIZE = 100000


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


def _view_columns(
    db: Session, template_version_id: int
) -> dict[str, models.DataColumn]:
    """Gets the unique columns associated with a `ViewTemplateVersion` by alias."""
    column_ref_ids = select(models.ViewTemplateColumnMember.ref_id).filter(
        models.ViewTemplateColumnMember.template_version_id == template_version_id
    )
    column_set_ids = select(models.ViewTemplateColumnSetMember.set_id).filter(
        models.ViewTemplateColumnSetMember.template_version_id == template_version_id
    )
    column_set_ref_ids = select(models.ColumnSetMember.ref_id).filter(
        models.ColumnSetMember.set_id.in_(column_set_ids)
    )

    column_ids_with_paths = db.execute(
        select(
            models.ColumnRef.path,
            models.Namespace.path.label("namespace"),
            models.ColumnRef.col_id,
        )
        .join(
            models.Namespace,
            models.Namespace.namespace_id == models.ColumnRef.namespace_id,
        )
        .where(models.ColumnRef.ref_id.in_(union(column_set_ref_ids, column_ref_ids)))
    ).all()
    column_ids = [row.col_id for row in column_ids_with_paths]

    # Determine the shortest unambiguous alias for each plan.
    namespaces_by_path = defaultdict(set)
    for row in column_ids_with_paths:
        namespaces_by_path[row.path].add(row.namespace)
    col_id_to_alias = {}
    for row in column_ids_with_paths:
        alias = (
            f"{row.namespace}__{row.path}"
            if len(namespaces_by_path[row.path]) > 1
            else row.path
        )
        col_id_to_alias[row.col_id] = alias

    raw_columns = (
        db.query(models.DataColumn)
        .filter(models.DataColumn.col_id.in_(column_ids))
        .all()
    )
    return {col_id_to_alias[col.col_id]: col for col in raw_columns}


@dataclass(frozen=True)
class ViewRenderContext:
    """Context for rendering a view's data and metadata."""

    view: models.View
    columns: dict[str, models.DataColumn]
    plans: list[models.Plan]
    plan_labels: list[str]
    plan_assignments: Sequence | None
    graph_edges: Sequence | None
    geo_meta: dict[int, models.ObjectMeta]
    geo_meta_ids: dict[str, int]  # by path
    geo_valid_from_dates: dict[str, datetime]

    # Bulk queries for `ogr2ogr`.
    geo_query: str
    internal_point_query: str


class CRView(NamespacedCRBase[models.View, schemas.ViewCreate]):
    def create(
        self,
        db: Session,
        *,
        obj_in: schemas.ViewCreate,
        obj_meta: models.ObjectMeta,
        namespace: models.Namespace,
        template: models.ViewTemplate | models.ViewTemplateVersion,
        locality: models.Locality,
        layer: models.GeoLayer,
        graph: models.Graph | None = None,
    ) -> Tuple[models.View, uuid.UUID]:
        """Creates a new view."""
        log.debug("TOP OF CR CREATE")
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

        log.debug("TO THE VIEW TEMPLATE VERSION")

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

        log.debug("TO THE COLUMNS")

        columns = _view_columns(db, template_version_id)
        log.debug("FOUND %d columns", len(columns))
        log.debug(str(columns))
        log.debug("TO THE GEO SET MEMBERS")
        geo_set_members = (
            db.query(models.GeoSetMember.geo_id)
            .filter(models.GeoSetMember.set_version_id == set_version_id)
            .subquery()
        )
        log.debug("TO THE VALUE COUNTS")
        value_counts = (
            db.query(
                models.ColumnValue.col_id,
                label("num_geos", func.count(models.ColumnValue.geo_id)),
            )
            .join(
                geo_set_members, geo_set_members.c.geo_id == models.ColumnValue.geo_id
            )
            .filter(
                models.ColumnValue.col_id.in_(bindparam("col_ids", expanding=True)),
                models.ColumnValue.valid_from <= valid_at,
                (
                    (models.ColumnValue.valid_to.is_(None))
                    | (models.ColumnValue.valid_to >= valid_at)
                ),
            )
            .params(col_ids=[col.col_id for col in columns.values()])
            .group_by(models.ColumnValue.col_id)
            .all()
        )
        value_counts_by_col = {group.col_id: group.num_geos for group in value_counts}
        bad_cols = []

        num_geos = len(
            db.query(models.GeoSetMember.geo_id)
            .filter(models.GeoSetMember.set_version_id == set_version_id)
            .all()
        )

        for column in columns.values():
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
                num_geos=num_geos,
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

    def all(self, db: Session, *, namespace: models.Namespace) -> list[models.View]:
        """Retrieves all views in a namespace."""
        return (
            db.query(models.View)
            .filter(models.View.namespace_id == namespace.namespace_id)
            .all()
        )

    def _create_render(
        self,
        db: Session,
        *,
        view: models.View,
        created_by: models.User,
        render_id: uuid.UUID,
        path: Path | str,
        status: ViewRenderStatus,
    ) -> models.ViewRender:
        """Creates view render metadata."""
        render = models.ViewRender(
            view_id=view.view_id,
            render_id=render_id,
            created_by=created_by.user_id,
            path=path,
            status=status,
        )
        db.add(render)
        db.flush()
        db.refresh(render)
        return render

    def cache_render(
        self,
        db: Session,
        *,
        view: models.View,
        created_by: models.User,
        render_id: uuid.UUID,
        path: Path | str,
    ) -> models.ViewRender:
        """Saves metadata for a successful render."""
        return self._create_render(
            db=db,
            view=view,
            created_by=created_by,
            render_id=render_id,
            path=path,
            status=ViewRenderStatus.SUCCEEDED,
        )

    def queue_render(
        self,
        db: Session,
        *,
        view: models.View,
        created_by: models.User,
        render_id: uuid.UUID,
        path: Path | str,
    ) -> models.ViewRender:
        """Adds a render to the job queue."""
        return self._create_render(
            db=db,
            view=view,
            created_by=created_by,
            render_id=render_id,
            path=path,
            status=ViewRenderStatus.PENDING,
        )

    def get_cached_render(
        self, db: Session, *, view: models.View
    ) -> models.ViewRender | None:
        """Retrieves metadata for a cached view render, if available."""
        return (
            db.query(models.ViewRender)
            .filter(
                models.ViewRender.view_id == view.view_id,
                models.ViewRender.status == ViewRenderStatus.SUCCEEDED,
            )
            .order_by(models.ViewRender.created_at.desc())
            .first()
        )

    def render(self, db: Session, *, view: models.View) -> ViewRenderContext:
        """Generates queries to retrieve view data.

        Used for bulk exports via `ogr2ogr`.
        """
        log.debug("TOP OF CR RENDER")
        columns = _view_columns(db, view.template_version_id)
        members_sub = (
            select(
                models.GeoSetMember.set_version_id,
                models.GeoSetMember.geo_id,
            )
            .filter(models.GeoSetMember.set_version_id == view.set_version_id)
            .subquery("members_sub")
        )
        geo_sub = select(models.Geography.geo_id, models.Geography.path).subquery(
            "geo_sub"
        )

        agg_selects = []
        column_labels = []
        col_ids = []
        for _, col in columns.items():
            agg_selects.append(
                func.max(column(COLUMN_TYPE_TO_VALUE_COLUMN[col.type]))
                .filter(models.ColumnValue.col_id == col.col_id)
                .label(col.canonical_ref.path)
            )
            column_labels.append(column(col.canonical_ref.path))
            col_ids.append(col.col_id)

        column_sub = (
            select(models.ColumnValue.geo_id, *agg_selects)
            .where(
                models.ColumnValue.col_id.in_(col_ids),
                models.ColumnValue.valid_from <= view.at,
                or_(
                    models.ColumnValue.valid_to.is_(None),
                    models.ColumnValue.valid_to >= view.at,
                ),
            )
            .group_by(models.ColumnValue.geo_id)
            .subquery("column_value")
        )

        timestamp_clauses = [
            models.GeoVersion.valid_from <= view.at,
            or_(
                models.GeoVersion.valid_to.is_(None),
                models.GeoVersion.valid_to >= view.at,
            ),
        ]

        ## included for reference: a version without subqueries.
        # geo_query=(
        #         select(models.Geography.path,
        #                models.GeoVersion.geography,
        #                *column_labels,
        #         ).join(models.GeoSetMember, models.GeoSetMember.geo_id==models.Geography.geo_id)
        #         .join(models.GeoVersion, models.GeoSetMember.geo_id==models.GeoVersion.geo_id)
        #         .join(column_sub, column_sub.c.geo_id==models.Geography.geo_id)
        #         .where(models.GeoSetMember.set_version_id == view.set_version_id, *timestamp_clauses)
        #     )

        # Add some casting the the geometry view and do the projection if needed
        if view.proj is not None:
            geo_col = geo_func.ST_Transform(
                models.GeoBin.geography.op("::")(literal_column("geometry")),
                int(view.proj.split(":")[1]),
            ).label("geography")
        elif view.loc.default_proj is not None:
            geo_col = geo_func.ST_Transform(
                models.GeoBin.geography.op("::")(literal_column("geometry")),
                int(view.loc.default_proj.split(":")[1]),
            ).label("geography")
        else:
            geo_col = models.GeoBin.geography.op("::")(
                literal_column("geometry")
            ).label("geography")

        geo_query = (
            select(
                geo_sub.c.path,
                geo_col,
                *column_labels,
            )
            .select_from(models.GeoVersion)
            .join(
                members_sub,
                members_sub.c.geo_id == models.GeoVersion.geo_id,
            )
            .join(geo_sub, geo_sub.c.geo_id == models.GeoVersion.geo_id)
            .join(
                models.GeoBin, models.GeoVersion.geo_bin_id == models.GeoBin.geo_bin_id
            )
        )

        geo_query = geo_query.join(
            column_sub, column_sub.c.geo_id == models.GeoVersion.geo_id
        )
        geo_query = geo_query.where(*timestamp_clauses)

        internal_point_query = (
            select(
                geo_sub.c.path,
                models.GeoBin.internal_point,
            )
            .select_from(models.GeoVersion)
            .join(
                members_sub,
                members_sub.c.geo_id == models.GeoVersion.geo_id,
            )
            .join(geo_sub, geo_sub.c.geo_id == models.GeoVersion.geo_id)
            .join(
                models.GeoBin, models.GeoVersion.geo_bin_id == models.GeoBin.geo_bin_id
            )
            .where(*timestamp_clauses)
        )

        plans, plan_labels, plan_assignments = self._plans(db, view)
        geo_meta_ids, geo_meta = self._geo_meta(db, view)
        geo_valid_from_dates = self._geo_valid_dates(db, view)

        cte = geo_query.cte(name="geo_full_table")
        final_query = select(literal_column("*")).select_from(cte)

        # Query generation: substitute in literals and remove the
        # ST_AsBinary() calls added by GeoAlchemy2.
        full_geo_query = re.sub(
            _ST_ASBINARY_REGEX,
            r"\1",
            str(
                final_query.compile(
                    dialect=postgresql.dialect(),
                    compile_kwargs={"literal_binds": True},
                )
            ),
        )

        log.debug("The new geo query is %s", full_geo_query)

        full_internal_point_query = re.sub(
            _ST_ASBINARY_REGEX,
            r"\1",
            str(
                internal_point_query.compile(
                    dialect=postgresql.dialect(),
                    compile_kwargs={"literal_binds": True},
                )
            ),
        )

        log.debug("The new internal point query is %s", full_internal_point_query)

        ret = ViewRenderContext(
            view=view,
            columns=columns,
            plans=plans,
            plan_labels=plan_labels,
            plan_assignments=plan_assignments,
            graph_edges=self._graph_edges(db, view),
            geo_meta=geo_meta,
            geo_meta_ids=geo_meta_ids,
            geo_valid_from_dates=geo_valid_from_dates,
            geo_query=full_geo_query,
            internal_point_query=full_internal_point_query,
        )
        return ret

    def _geo_meta(
        self, db: Session, view: models.View
    ) -> tuple[dict[str, int], dict[int, models.ObjectMeta]]:
        """Gets object metadata associated with a view's geographies.

        Returns:
            (1) Mapping from geography paths to metadata IDs.
            (2) Mapping from metadata IDs to metadata objects.
        """
        members_sub = (
            select(models.GeoSetMember.geo_id)
            .filter(models.GeoSetMember.set_version_id == view.set_version_id)
            .subquery()
        )
        raw_geo_meta_ids = db.execute(
            select(models.Geography.path, models.Geography.meta_id).join(
                members_sub, members_sub.c.geo_id == models.Geography.geo_id
            )
        ).fetchall()
        geo_meta_ids = {row.path: row.meta_id for row in raw_geo_meta_ids}

        distinct_meta_ids = set(geo_meta_ids.values())
        raw_distinct_meta = (
            db.query(models.ObjectMeta)
            .where(models.ObjectMeta.meta_id.in_(distinct_meta_ids))
            .all()
        )
        distinct_meta = {meta.meta_id: meta for meta in raw_distinct_meta}

        return geo_meta_ids, distinct_meta

    def _geo_valid_dates(self, db: Session, view: models.View) -> dict[str, datetime]:
        """Gets the valid dates for each geometry.

        Returns:
            A dictionary mapping geometry IDs to valid dates.
        """

        query = (
            select(models.Geography.path, models.GeoVersion.valid_from)
            .join(
                models.GeoSetMember,
                models.Geography.geo_id == models.GeoSetMember.geo_id,
            )
            .join(
                models.GeoVersion, models.Geography.geo_id == models.GeoVersion.geo_id
            )
            .where(models.GeoSetMember.set_version_id == view.set_version_id)
        )

        result = db.execute(query)

        return {row.path: row.valid_from for row in result}

    def _plans(
        self, db: Session, view: models.View
    ) -> tuple[list[models.Plan], list[str], Sequence | None]:
        """Gets plans associated with a view.

        Returns:
            (1) A list of plans compatible with the view.
                (These plans also satisfy the view's public join constraint.)
            (2) A list of column labels for the plans.
            (3) A database iterator for the plan assignments, if any assignments
                are available.
        """
        # Get plans that existed when the view was created.
        plans = (
            db.query(models.Plan)
            .filter(
                models.Plan.set_version_id == view.set_version_id,
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

        # Get plan assignments as a table.
        plan_labels = []
        if visible_plans:
            # Determine the shortest unambiguous alias for each plan.
            namespaces_by_path = defaultdict(set)
            for plan in visible_plans:
                namespaces_by_path[plan.path].add(plan.namespace.path)

            # Generate query clauses for each plan.
            plan_subs = []
            for plan in visible_plans:
                label = (
                    f"{plan.namespace.path}__{plan.path}"
                    if len(namespaces_by_path[plan.path]) > 1
                    else plan.path
                )
                plan_labels.append(label)
                plan_subs.append(
                    select(
                        models.PlanAssignment.geo_id, models.PlanAssignment.assignment
                    )
                    .where(
                        models.PlanAssignment.plan_id == plan.plan_id,
                    )
                    .subquery()
                )

            geo_sub = select(models.Geography.geo_id, models.Geography.path).subquery()
            members_sub = (
                select(models.GeoSetMember.geo_id)
                .filter(models.GeoSetMember.set_version_id == view.set_version_id)
                .subquery()
            )
            plan_cols = [
                plan_sub.c.assignment.label(plan_label)
                for plan_sub, plan_label in zip(plan_subs, plan_labels)
            ]
            plan_assignment_query = (
                select(models.GeoVersion.geo_id, geo_sub.c.path, *plan_cols)
                .join(geo_sub, geo_sub.c.geo_id == models.GeoVersion.geo_id)
                .join(members_sub, members_sub.c.geo_id == models.GeoVersion.geo_id)
            )
            for plan_sub in plan_subs:
                plan_assignment_query = plan_assignment_query.outerjoin(
                    plan_sub,
                    plan_sub.c.geo_id == models.GeoVersion.geo_id,
                )
            plan_assignments = db.execute(plan_assignment_query).fetchall()
        else:
            plan_assignments = None

        return visible_plans, plan_labels, plan_assignments

    def _graph_edges(self, db: Session, view: models.View) -> Sequence | None:
        """Gets graph edges by path, if applicable."""
        if view.graph_id is None:
            return None

        path_sub_1 = select(models.Geography.geo_id, models.Geography.path).subquery()
        path_sub_2 = select(models.Geography.geo_id, models.Geography.path).subquery()
        graph_edges_query = (
            select(
                path_sub_1.c.path.label("path_1"),
                path_sub_2.c.path.label("path_2"),
                models.GraphEdge.weights,
            )
            .join(
                path_sub_1,
                path_sub_1.c.geo_id == models.GraphEdge.geo_id_1,
            )
            .join(
                path_sub_2,
                path_sub_2.c.geo_id == models.GraphEdge.geo_id_2,
            )
            .where(
                models.GraphEdge.graph_id == view.graph_id,
            )
        )

        return db.execute(graph_edges_query).fetchall()


view = CRView(models.View)
