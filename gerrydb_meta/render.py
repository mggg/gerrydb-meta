"""Extended GeoPackage rendering of views."""
import logging
import sqlite3
import subprocess
import tempfile
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

import orjson as json

from gerrydb_meta.crud.view import ViewRenderContext
from gerrydb_meta.schemas import ObjectMeta, ViewMeta

# For bulk exports, we wrap the command-line utility `ogr2ogr` (distributed with GDAL)
# to generate a GeoPackage with geographies and tabular data directly from the
# PostGIS database; SQLAlchemy is only used to generate the query that `ogr2ogr` runs.
# We then treat the GeoPackage export as a regular SQLite database and inject metadata.
#
# The CLI/subprocess dependency is slightly annoying (and potentially a cause for
# security concerns--ideally, ogr2ogr should be granted only read access to the database),
# but empirical evidence suggests that using `ogr2ogr` is *much* faster and less
# memory-intensive than loading geographies with SQLAlchemy/GeoAlchemy.

log = logging.getLogger()


class RenderError(Exception):
    """Raised when rendering a view fails."""


def _init_base_gpkg_extensions(conn: sqlite3.Connection, layer_name: str) -> None:
    conn.execute(
        """
        CREATE TABLE gerrydb_view_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE gerrydb_geo_meta (
            meta_id INTEGER PRIMARY KEY,
            value   TEXT    NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE gerrydb_geo_meta_xref (
            path    TEXT PRIMARY KEY REFERENCES {layer_name}(path),
            meta_id BLOB NOT NULL    REFERENCES gerrydb_geo_meta(meta_id)
        )
        """
    )

    # gpkg_data_columns_sql table definition:
    # https://www.geopackage.org/spec/#gpkg_data_columns_sql
    conn.execute(
        """
        CREATE TABLE gpkg_data_columns (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            name TEXT,
            title TEXT,
            description TEXT,
            mime_type TEXT,
            constraint_name TEXT,
            CONSTRAINT pk_gdc PRIMARY KEY (table_name, column_name),
            CONSTRAINT gdc_tn UNIQUE (table_name, name)
        )
        """
    )
    # gpkg_extensions table definition: http://www.geopackage.org/spec/#_gpkg_extensions
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gpkg_extensions (
            table_name     TEXT,
            column_name    TEXT,
            extension_name TEXT NOT NULL,
            definition     TEXT NOT NULL,
            scope          TEXT NOT NULL,
            CONSTRAINT ge_tce UNIQUE (table_name, column_name, extension_name)
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO gpkg_extensions
        (table_name, column_name, extension_name, definition, scope)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                "gerrydb_view_meta",
                None,
                "mggg_gerrydb",
                (
                    "JSON-formatted metadata for the view's "
                    "tabular, geographic, and graph data."
                ),
                "read-write",
            ),
            (
                "gerrydb_geo_meta",
                None,
                "mggg_gerrydb",
                "JSON-formatted metadata for the view's geographies.",
                "read-write",
            ),
            (
                "gerrydb_geo_meta_xref",
                None,
                "mggg_gerrydb",
                "Mapping between geographies and metadata objects.",
                "read-write",
            ),
        ],
    )
    conn.commit()


def _init_gpkg_graph_extension(conn: sqlite3.Connection, layer_name: str):
    """Initializes a graph edge table in a GeoPackage."""
    conn.execute(
        f"""
        CREATE TABLE gerrydb_graph_edge (
            path_1  TEXT NOT NULL REFERENCES {layer_name}(path),
            path_2  TEXT NOT NULL REFERENCES {layer_name}(path),
            weights TEXT,
            CONSTRAINT unique_edges UNIQUE (path_1, path_2)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE gerrydb_graph_node_area (
            path  TEXT PRIMARY KEY REFERENCES {layer_name}(path),
            area  REAL NOT NULL
        )
        """
    )

    conn.executemany(
        """
        INSERT INTO gpkg_extensions
        (table_name, column_name, extension_name, definition, scope)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                "gerrydb_graph_edge",
                None,
                "mggg_gerrydb",
                "Edges of a dual graph (adjacency graph) of the view's geographies.",
                "read-write",
            ),
            (
                "gerrydb_graph_node_area",
                None,
                "mggg_gerrydb",
                "Node areas of a dual graph (adjacency graph) of the view's geographies.",
                "read-write",
            ),
        ],
    )
    conn.commit()


def _init_gpkg_plans_extension(
    conn: sqlite3.Connection, layer_name: str, columns: list[str]
):
    """Initializes a plan assignments table in a GeoPackage."""
    table_columns = " TEXT,\n".join(columns) + " TEXT\n"
    conn.execute(
        f"""
        CREATE TABLE gerrydb_plan_assignment (
            path TEXT PRIMARY KEY REFERENCES {layer_name}(path),
            {table_columns} 
        )
        """
    )
    conn.execute(
        """
        INSERT INTO gpkg_extensions
        (table_name, column_name, extension_name, definition, scope)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            "gerrydb_plan_assignment",
            None,
            "mggg_gerrydb",
            (
                "District assignments by geography for "
                "districting plans associated with the view."
            ),
            "read-write",
        ),
    )
    conn.commit()


def view_to_gpkg(
    context: ViewRenderContext, db_url: str
) -> tuple[uuid.UUID, Path, TemporaryDirectory]:
    """Renders a view (with metadata) to a GeoPackage."""
    render_uuid = uuid.uuid4()
    temp_dir = tempfile.TemporaryDirectory()
    gpkg_path = Path(temp_dir.name) / f"{render_uuid.hex}.gpkg"

    geo_layer_name = context.view.path
    internal_point_layer_name = f"{geo_layer_name}__internal_points"

    if context.view.proj is not None:
        proj_args = ["-t_srs", context.view.proj]
    if context.view.loc.default_proj is not None:
        proj_args = ["-t_srs", context.view.loc.default_proj]
    else:
        proj_args = []  # leave in original projection (conventionally EPSG:4269)

    base_args = [
        "-f",
        "GPKG",
        str(gpkg_path),
        f"PG:{db_url}",
        *proj_args,
    ]

    try:
        subprocess.run(
            [
                "ogr2ogr",
                *base_args,
                "-sql",
                context.geo_query,
                "-nln",
                geo_layer_name,
            ],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError:
        # Watch out for accidentally leaking credentials via logging here.
        log.error("Failed to export view with ogr2ogr. Query: %s", context.geo_query)
        raise RenderError("Failed to render view: geography query failed.")

    conn = sqlite3.connect(gpkg_path)
    try:
        geo_row_count = conn.execute(
            f"SELECT COUNT(*) FROM {geo_layer_name}"
        ).fetchone()[0]
    except sqlite3.OperationalError as ex:
        raise RenderError(
            "Failed to render view: geographic layer not found in GeoPackage.",
        ) from ex
    if geo_row_count != context.view.num_geos:
        # Validate inner joins.
        raise RenderError(
            f"Failed to render view: expected {context.view.num_geos} geographies "
            f"in layer, got {geo_row_count} geographies."
        )

    try:
        subprocess.run(
            [
                "ogr2ogr",
                *base_args,
                "-update",
                "-sql",
                context.internal_point_query,
                "-nln",
                internal_point_layer_name,
                "-nlt",
                "POINT",
            ],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError:
        # Watch out for accidentally leaking credentials via logging here.
        log.error(
            "Failed to export view with ogr2ogr. Query: %s",
            context.internal_point_query,
        )
        raise RenderError("Failed to render view: internal point query failed.")

    try:
        internal_point_row_count = conn.execute(
            f"SELECT COUNT(*) FROM {internal_point_layer_name}"
        ).fetchone()[0]
    except sqlite3.OperationalError as ex:
        raise RenderError(
            "Failed to render view: internal point layer not found in GeoPackage.",
        ) from ex
    if internal_point_row_count != context.view.num_geos:
        # Validate inner joins.
        raise RenderError(
            f"Failed to render view: expected {context.view.num_geos} points "
            f"in layer, got {geo_row_count} geographies."
        )

    # Create indices and references on paths.
    conn.execute(f"CREATE UNIQUE INDEX idx_geo_path ON {geo_layer_name}(path)")
    conn.execute(
        "CREATE UNIQUE INDEX idx_internal_point_path "
        f"ON {internal_point_layer_name}(path)"
    )

    ## Add extended (non-geographic) data.
    _init_base_gpkg_extensions(conn, geo_layer_name)

    conn.executemany(
        "INSERT INTO gerrydb_view_meta (key, value) VALUES (?, ?)",
        (
            (key, json.dumps(value).decode("utf-8"))
            for key, value in ViewMeta.from_orm(context.view).dict().items()
        ),
    )
    conn.executemany(
        (
            "INSERT INTO gpkg_data_columns (table_name, column_name, description) "
            "VALUES (?, ?, ?)"
        ),
        (
            (geo_layer_name, alias, col.description)
            for alias, col in context.columns.items()
        ),
    )

    # Insert geographic metadata objects.
    db_meta_id_to_gpkg_meta_id = {}
    for db_id, meta in context.geo_meta.items():
        cur = conn.execute(
            "INSERT INTO gerrydb_geo_meta (value) VALUES (?)",
            (json.dumps(ObjectMeta.from_orm(meta).dict()).decode("utf-8"),),
        )
        db_meta_id_to_gpkg_meta_id[db_id] = cur.lastrowid

    conn.executemany(
        "INSERT INTO gerrydb_geo_meta_xref (path, meta_id) VALUES (?, ?)",
        (
            (path, db_meta_id_to_gpkg_meta_id[db_id])
            for path, db_id in context.geo_meta_ids.items()
        ),
    )

    if context.graph_edges is not None:
        _init_gpkg_graph_extension(conn, geo_layer_name)
        conn.executemany(
            "INSERT INTO gerrydb_graph_edge (path_1, path_2, weights) VALUES (?, ?, ?)",
            (
                (edge.path_1, edge.path_2, json.dumps(edge.weights).decode("utf-8"))
                for edge in context.graph_edges
            ),
        )
        conn.executemany(
            "INSERT INTO gerrydb_graph_node_area (path, area) VALUES (?, ?)",
            ((node.path, node.area) for node in context.graph_areas),
        )

    if context.plan_assignments is not None:
        _init_gpkg_plans_extension(conn, geo_layer_name, context.plan_labels)
        cols = ["path", *context.plan_labels]
        placeholders = ", ".join(["?"] * len(cols))

        conn.executemany(
            (
                f"INSERT INTO gerrydb_plan_assignment ({', '.join(cols)}) "
                f"VALUES ({placeholders})"
            ),
            ([getattr(row, col) for col in cols] for row in context.plan_assignments),
        )

    conn.commit()
    conn.close()

    return render_uuid, gpkg_path, temp_dir
