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


def _init_gpkg_extensions(conn: sqlite3.Connection) -> None:
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
        CREATE TABLE gerrydb_graph_edge (
            path_1 TEXT NOT NULL, 
            path_2 TEXT NOT NULL, 
            CONSTRAINT unique_edges UNIQUE (path_1, path_2)
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
                "JSON-formatted metadata for the view's geographic and tabular data.",
                "read-write",
            ),
            (
                "gerrydb_graph_edge",
                None,
                "mggg_gerrydb",
                "Edges in a dual graph (adjacency graph) of the view's geographies.",
                "read-write",
            ),
        ],
    )


def view_to_gpkg(
    context: ViewRenderContext, db_url: str
) -> tuple[uuid.UUID, Path, TemporaryDirectory]:
    """Renders a view (with metadata) to a GeoPackage."""
    render_uuid = uuid.uuid4()
    temp_dir = tempfile.TemporaryDirectory()
    gpkg_path = Path(temp_dir.name) / f"{render_uuid.hex}.gpkg"

    try:
        # TODO: reproject?
        subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "GPKG",
                str(gpkg_path),
                f"PG:{db_url}",
                "-sql",
                context.query,
            ],
            check=True,
        )
    except subprocess.CalledProcessError:
        # Watch out for accidentally leaking credentials via logging here.
        log.error("Failed to export view with ogr2ogr. Query: %s", context.query)
        raise RenderError("Failed to render view.")

    conn = sqlite3.connect(gpkg_path)
    _init_gpkg_extensions(conn)
    return render_uuid, gpkg_path, temp_dir
