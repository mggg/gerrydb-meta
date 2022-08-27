"""Downloads and imports county shapefiles (2010 and 2020) from the U.S. Census."""
import logging
from pathlib import Path

import click
import requests
import us
import geopandas as gpd
import yaml

from cherrydb_meta.etl import DataSource, ETLContext, config_logger
from cherrydb_meta.models import AliasKind, Location, LocationKind

CONFIG_PATH = Path(__file__).resolve().parent / "config" / "county.yaml"
TOP_LEVEL_NOTES = (
    "Imported from the `us` Python package "
    "(https://github.com/unitedstates/python-us) "
    "by the `census_pl94_171_counties` ETL script."
)

log = logging.getLogger()

def upsert_top_level_locs(ctx: ETLContext) -> list[Location]:
    """Upserts location metadata for all U.S. states and territories."""
    log.info("Upserting top-level locations (U.S. root + states and territories)...")
    locs = []
    with ctx.upserter() as upserter:
        root_loc = upserter.location(
            name="United States of America",
            kind=LocationKind.COUNTRY,
            aliases=[("US", AliasKind.POSTAL), ("United States", AliasKind.CUSTOM)],
            notes=TOP_LEVEL_NOTES,
        )
        log.info("Upserted root: %s", root_loc)
        locs.append(root_loc)

        for state in us.states.STATES_AND_TERRITORIES:
            state_aliases = [
                (state.fips, AliasKind.FIPS),
                (state.abbr, AliasKind.POSTAL),
            ]
            if state.ap_abbr is not None and state.ap_abbr != state.name:
                # Some states (e.g. Texas) have AP abbreviations that are identical
                # to their full names, in which case an AP alias is redundant.
                # Furthermore, some territories (e.g. American Samoa) don't have AP
                # abbreviations.
                state_aliases.append((state.ap_abbr, AliasKind.ASSOCIATED_PRESS))
            kind = LocationKind.TERRITORY if state.is_territory else LocationKind.STATE
            state_loc = upserter.location(
                name=state.name,
                kind=kind,
                aliases=state_aliases,
                parent=root_loc,
                notes=TOP_LEVEL_NOTES,
            )
            log.info("Upserted state/territory: %s", state_loc)
            locs.append(state_loc)
    log.info("Upserted top-level locations.")
    return locs

def upsert_county_locs(ctx: ETLContext, county_gdf: gpd.GeoDataFrame):
    pass

def upsert_county_geo(ctx: ETLContext, county_gdf: gpd.GeoDataFrame):
    pass

@click.command()
@click.option("--version", required=True, type=click.Choice(["2010", "2020"]))
def main(version: str):
    with open(CONFIG_PATH) as config_fp:
        raw_config = yaml.safe_load(config_fp)
    versions = [DataSource(**raw) for raw in raw_config["versions"]]
    ctx = ETLContext.from_env()
    top_locs = upsert_top_level_locs(ctx)


if __name__ == "__main__":
    config_logger(log)
    main()
