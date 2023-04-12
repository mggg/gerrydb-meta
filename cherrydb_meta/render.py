"""GeoJSON/Extended GeoPackage rendering of views."""
import logging
import sqlite3
import subprocess
import tempfile
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

from cherrydb_meta.crud.view import ViewRenderContext

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


def _init_base_gpkg_extensions(conn: sqlite3.Connection) -> None:
    # gpkg_extensions table definition: http://www.geopackage.org/spec/#_gpkg_extensions
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
            uuid  BLOB PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
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
                "JSON-formatted metadata for the view's geographies." "read-write",
            ),
        ],
    )


def _init_gpkg_graph_extension(conn: sqlite3.Connection):
    """Initializes a graph edge table in a GeoPackage."""
    conn.execute(
        """
        CREATE TABLE gerrydb_graph_edge (
            path_1  TEXT NOT NULL, 
            path_2  TEXT NOT NULL,
            weights TEXT,
            CONSTRAINT unique_edges UNIQUE (path_1, path_2)
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
            "gerrydb_graph_edge",
            None,
            "mggg_gerrydb",
            "Edges in a dual graph (adjacency graph) of the view's geographies.",
            "read-write",
        ),
    )


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

    # Add extended (non-geographic) data.
    _init_base_gpkg_extensions(conn)

    if context.graph_edges is not None:
        _init_gpkg_graph_extension(conn)
        conn.executemany(
            "INSERT INTO gerrydb_graph_edge (path_1, path_2, weights) VALUES (?, ?, ?)",
            ((edge.path_1, edge.path_2, edge.weights) for edge in context.graph_edges),
        )

    #if context.plans:
    #    _init_gpkg_plans_extension(conn)

    return render_uuid, gpkg_path, temp_dir
