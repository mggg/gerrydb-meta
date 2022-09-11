"""Upserts Census geographies for a state."""
import logging
from pathlib import Path

import click
import us
import geopandas as gpd
import yaml

from cherrydb_meta.etl import DataSource, ETLContext, config_logger
from cherrydb_meta.models import Location, GeoUnit

CONFIG_ROOT = Path(__file__).resolve().parent / "config"
SPINE_LEVELS = [GeoUnit.COUNTY]  # , GeoUnit.TRACT, GeoUnit.BG, GeoUnit.BLOCK]
AUX_LEVELS = []  # GeoUnit.VTD, GeoUnit.COUSUB, GeoUnit.PLACE]


log = logging.getLogger()


@click.command()
@click.option("--state", required=True)
@click.option("--version", required=True, type=click.Choice(["2010", "2020"]))
def main(state: str, version: str):
    sources: dict[GeoUnit, DataSource] = {}
    for level in SPINE_LEVELS + AUX_LEVELS:
        with open(CONFIG_ROOT / (level.value + ".yaml")) as config_fp:
            raw_config = yaml.safe_load(config_fp)
        versions = {raw["version"]: DataSource(**raw) for raw in raw_config["versions"]}
        sources[level] = versions[version]

    ctx = ETLContext.from_env()
    state_meta = us.states.lookup(state)
    with ctx.upserter() as upserter:
        loc = (
            upserter.session.query(Location)
            .filter(Location.name == state_meta.name)
            .first()
        )
        if loc is None:
            raise ValueError("State does not have a location yet!")

        universe = upserter.universe(
            name=f"census_{version}",
            description=f"U.S. Census {version} PL 94-171 release.",
        )

        canonical_id_to_node_id = {}
        for level in SPINE_LEVELS:
            source_url = sources[level].source.format(state_meta.fips)
            log.info("Fetching shapefile from %s...", source_url)
            level_gdf = gpd.read_file(source_url)
            log.info(
                "Upserting geography (state: %s, level: %s)...", state_meta.name, level
            )
            nodes = upserter.geography_from_df(
                gdf=level_gdf,
                loc=loc,
                universe=universe,
                unit=level,
                version=version,
                source_meta=sources[level],
                source_url=source_url,
            )
            for node in nodes:
                canonical_id_to_node_id[node.canonical_id] = node.node_id

        # TODO: Construct geographic hierarchy from prefixes.


if __name__ == "__main__":
    config_logger(log)
    main()
