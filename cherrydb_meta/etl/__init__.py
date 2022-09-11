"""Common ETL functionality and data models."""
import logging
import os
import sys
import warnings
import pandas as pd
import geopandas as gpd
from collections import Counter
from contextlib import contextmanager
from typing import Any, Collection, Generator, Optional, Union

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, HttpUrl, root_validator, validator
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session as SessionType
from sqlalchemy.orm import sessionmaker
from geoalchemy2.shape import from_shape
from utm import latlon_to_zone_number

from cherrydb_meta.models import (
    AliasKind,
    GeoAttrAlias,
    Geography,
    GeoNode,
    GeoUnit,
    GeoMember,
    GeoUniverse,
    Location,
    LocationAlias,
    LocationKind,
    ObjectMeta,
    User,
    GeoAttr,
    GeoAttrBool,
    GeoAttrFloat,
    GeoAttrInt,
    GeoAttrStr,
    GeoAttrType,
)

log = logging.getLogger()

PY_TYPE_TO_ATTR_TABLE = {
    bool: GeoAttrBool,
    float: GeoAttrFloat,
    int: GeoAttrInt,
    str: GeoAttrStr,
}

PY_TYPE_TO_ATTR_TYPE = {
    bool: GeoAttrType.BOOL,
    float: GeoAttrType.FLOAT,
    int: GeoAttrType.INT,
    str: GeoAttrType.STR,
}


class BaseModel(PydanticBaseModel):
    """Base model for ETL configuration."""

    class Config:
        allow_mutation = False
        arbitrary_types_allowed = True


class ETLUpserter(PydanticBaseModel):
    """Wraps ETL upsert operations."""

    session: SessionType
    user: User
    notes: Optional[str] = None
    meta: Optional[ObjectMeta] = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        # Create metadata for a series of upsert operations.
        session = data["session"]
        meta = ObjectMeta(created_by=data["user"].user_id, notes=data.get("notes"))
        session.add(meta)
        session.flush()
        session.refresh(meta)
        data["meta"] = meta
        super().__init__(**data)

    def refresh_meta(self, notes: str) -> None:
        """Creates a new metadata object to use for future upserts."""
        self.meta = ObjectMeta(created_by=self.user.user_id, notes=notes)
        self.session.add(self.meta)
        self.session.flush()
        self.session.refresh(self.meta)

    def _upsert(self, cls: type, values: dict[str, Any], cols: Collection[str]) -> Any:
        """Executes a conservative upsert of a single row.

        For core data (e.g. Census tables), we maintain a strong immutability invariant:
        once data from a trusted source has been imported, it cannot be changed.
        Rerunning ETL scripts (for instance, due to a partial failure) should
        not affect previously inserted data and object metadata.

        PostgreSQL supports such semantics via the `ON CONFLICT DO NOTHING` clause.
        However, no data is returned from an INSERT statement with this clause,
        even if a `RETURNING` clause is present. Thus, we first attempt to upsert
        in do-nothing-on-conflict mode; if values are returned (a new row was created),
        we return an ORM object retrieved by the primary key of the new row.
        If no values are returned (the row already existed), we query for an existing
        row separately, using the columns specified in `cols` to uniquely identify the
        row.

        Some upsert operations may lead to concurrency issues (see
        https://stackoverflow.com/a/42217872). We do not currently make any strong
        concurrency guarantees; we assume that any ETL scripts executed in parallel
        are mostly independent. We may address these issues with explicit locking
        if the need eventually arises.

        If `cols` do not uniquely identify a row, the behavior of this operation
        is not guaranteed to be deterministic.

        Args:
            cls:
                ORM model associated with the upsert operation.
                Must have a single primary key.
            values: Row values to upsert.
            cols:
                Columns used to uniquely identify a previously inserted row,
                if necessary.

        Returns:
            An ORM model (with class `cls`) or SQLAlchemy row containing `values`.
        """
        stmt = insert(cls).values(**values).on_conflict_do_nothing().returning(cls)
        result = self.session.execute(stmt).first()
        if result is None:
            select_stmt = self.session.query(cls)
            for col in cols:
                select_stmt = select_stmt.filter(getattr(cls, col) == values[col])
            return select_stmt.first()
        pk_name = inspect(cls).primary_key[0].name
        return self.session.query(cls).get(getattr(result, pk_name))

    def universe(
        self,
        name: str,
        description: Optional[str] = None,
    ) -> GeoUniverse:
        """Upserts a geographic universe (e.g. `census_2010`)."""
        log.info("Upserting geographic universe (name = %s)...", name)
        universe = self._upsert(
            GeoUniverse,
            dict(
                name=name,
                description=description,
                meta_id=self.meta.meta_id,
            ),
            ("name",),
        )
        log.info("Upserted %s.", universe)
        return universe

    def location(
        self,
        name: str,
        kind: LocationKind,
        aliases: list[tuple[str, AliasKind]],
        parent: Optional[Location] = None,
    ) -> Location:
        """Upserts a location (with aliases)."""
        log.info("Upserting location (name = %s, kind = %s)...", name, kind)
        loc = self._upsert(
            Location,
            dict(
                name=name,
                kind=kind,
                parent_id=parent.loc_id if parent else None,
                meta_id=self.meta.meta_id,
            ),
            ("name",),
        )
        log.info("Upserted %s.", loc)

        alias_values = [
            {
                "name": name,
                "kind": kind,
                "loc_id": loc.loc_id,
                "meta_id": self.meta.meta_id,
            }
            for name, kind in aliases
        ]
        alias_stmt = insert(LocationAlias).values(alias_values).on_conflict_do_nothing()
        self.session.execute(alias_stmt)
        log.info("Upserted %d aliases for %s.", len(aliases), loc)
        return loc

    def geography_from_df(
        self,
        gdf: gpd.GeoDataFrame,
        loc: Location,
        unit: GeoUnit,
        universe: GeoUniverse,
        version: str,
        source_meta: "DataSource",
        source_url: Optional[str] = None,
        description: Optional[str] = None,
        to_utm: bool = True,
    ) -> list[GeoNode]:
        """Upserts a `GeoDataFrame` as a `Geography`.

        Also upserts non-geographical attributes associated with
        the DataFrame, as filtered and mapped by `source_meta`.
        """
        filtered_gdf = source_meta.filter_and_rename(gdf)
        if to_utm:
            # Modified from gerrychain.graph.geo.
            wgs_df = gdf.to_crs("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
            with warnings.catch_warnings():
                # Ignore geometry warnings associated with using a geographic projection.
                warnings.filterwarnings("ignore", category=UserWarning)
                utm_counts = Counter(
                    latlon_to_zone_number(point.y, point.x)
                    for point in wgs_df["geometry"].centroid
                )
            zone = utm_counts.most_common(1)[0][0]
            proj_gdf = filtered_gdf.to_crs(
                f"+proj=utm +zone={zone} +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
            )
        else:
            proj_gdf = filtered_gdf

        srid = gdf.crs.to_epsg()
        existing_geo = (
            self.session.query(Geography)
            .filter(Geography.loc_id == loc.loc_id)
            .filter(Geography.universe_id == universe.universe_id)
            .filter(Geography.unit == unit)
            .filter(Geography.version == version)
            .first()
        )
        if existing_geo is None:
            log.info(
                "Geography %s already exists. Skipping further inserts.", existing_geo
            )
            return

        geography = Geography(
            loc_id=loc.loc_id,
            universe_id=universe.universe_id,
            meta_id=self.meta.meta_id,
            unit=unit,
            version=version,
            srid=srid,
            description=description,
            source_url=source_url,
        )
        self.session.add(geography)
        log.info("Inserted %s.")

        # TODO: skip if the geography already existed.
        canonical_id_col = source_meta.special.canonical_id
        nodes = [
            GeoNode(
                canonical_id=getattr(row, canonical_id_col),
                geometry=from_shape(getattr(row, "geometry"), srid=srid),
            )
            for row in proj_gdf.itertuples()
        ]
        self.session.add_all(nodes)
        log.info("Inserted %d geographic nodes.", len(nodes))
        self.session.flush()
        self.session.refresh(nodes)
        self.session.refresh(geography)

        # Associate nodes with the geography.
        self.session.add_all(
            [GeoMember(geo_id=geography.geo_id, node_id=node.node_id) for node in nodes]
        )
        log.info("Associated %d geographic nodes with %s.", len(nodes), geography)

        self.attrs_from_df(
            df=gdf.drop("geometry", axis=1),
            source_meta=source_meta,
            nodes=nodes,
        )
        return nodes

    def attrs_from_df(
        self,
        df: pd.DataFrame,
        source_meta: "DataSource",
        universe: GeoUniverse,
        nodes: list[GeoNode],
    ) -> None:
        attrs_meta = {attr_meta.target: attr_meta for attr_meta in source_meta.attrs}
        df = df.set_index(source_meta.special.canonical_id)
        attrs = []
        canonical_id_to_node_id = {node.canonical_id: node.node_id for node in nodes}
        for attr_name, attr_meta in attrs_meta.items():
            attr_values = {
                canonical_id_to_node_id[canonical_id]: val
                for canonical_id, val in df[attr_name].items()
            }
            attrs.append(self.attr(attr_meta, universe, attr_values))
        return attrs

    def attr(
        self,
        meta: "DataSourceAttr",
        universe: GeoUniverse,
        values: dict[int, Union[str, int, bool, float]],
    ) -> GeoAttr:
        """Upserts an attribute with aliases and values."""
        log.info("Upserting %s...", meta.target)
        val_types = set(type(val) for val in values.values())
        if len(val_types) != 1:
            raise ValueError(
                "Expected attribute values to have homogenous type, but "
                f"found types {val_types}."
            )
        val_type = next(iter(val_types))
        if val_type not in PY_TYPE_TO_ATTR_TABLE:
            raise ValueError(
                f"Attribute values have type '{val_type}', but only "
                f"{', '.join(str(t) for t in PY_TYPE_TO_ATTR_TABLE)} are allowed.",
            )

        attr = self._upsert(
            GeoAttr,
            dict(
                universe_id=universe.universe_id,
                name=meta.target,
                description=meta.description,
                type=PY_TYPE_TO_ATTR_TYPE[val_type],
                meta_id=self.meta.meta_id,
            ),
            (
                "name",
                "universe_id",
            ),
        )
        self.session.flush()
        self.session.refresh(attr)
        log.info("Upserted %s.", attr)

        for alias_name in meta.aliases:
            attr_alias = self._upsert(
                GeoAttrAlias,
                dict(
                    attr_id=attr.attr_id,
                    universe_id=universe.universe_id,
                    name=alias_name,
                    meta_id=self.meta.meta_id,
                ),
            )
            log.info("Upserted alias for %s: %s", attr, attr_alias)

        attr_table = PY_TYPE_TO_ATTR_TABLE[val_type]
        insert(attr_table).values(
            [
                dict(node_id=node_id, attr_id=attr.atr_id, val=val)
                for node_id, val in values.items()
            ]
        ).on_conflict_do_nothing()
        log.info("Upserted %d values for %s.", len(values), attr)
        return attr


class ETLContext(BaseModel):
    """Context for ETL operations."""

    engine: Engine
    user: Optional[User] = None

    @contextmanager
    def session(self) -> Generator[SessionType, None, None]:
        """Yields a database session.

        Commits the outstanding transaction if no errors exceptions
        occur in the context manager scope;
        otherwise, rolls back the outstanding transaction.
        """
        session = sessionmaker(self.engine)()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def upserter(
        self, notes: Optional[str] = None
    ) -> Generator[SessionType, None, None]:
        """Yields an upserter, which contains a database session.

        Commits the outstanding transaction if no errors exceptions
        occur in the context manager scope;
        otherwise, rolls back the outstanding transaction.

        Args:
            notes: Metadata notes to associate with all upserts.

        Raises:
            ValueError: No `user` is available.
        """
        if self.user is None:
            raise ValueError("Cannot create an upserter without an ETL user.")
        with self.session() as session:
            yield ETLUpserter(session=session, user=self.user, notes=notes)

    @classmethod
    def from_env(cls) -> "ETLContext":
        """Returns an ETL context created from environment variable settings.

        Environment variables:
            CHERRY_DATABASE_URI: PostgreSQL database URI for ETL database.
            CHERRY_USER: Email of the CherryDB user to associate with ETL operations.

        Raises:
            ValueError: The specified `CHERRY_USER` is not present in the database.
        """
        engine = create_engine(os.getenv("CHERRY_DATABASE_URI"))
        email = os.getenv("CHERRY_USER")
        if email is None:
            user = None
        else:
            session = sessionmaker(engine)()
            user = session.query(User).filter(User.email == email).first()
            if user is None:
                raise ValueError(f"No user found with email {email}.")
        return cls(engine=engine, user=user)


class DataSourceAttr(BaseModel):
    """An attribute (column) in a data source for the ETL pipeline."""

    source: str = Field(..., description="Original name of the attribute.")
    target: str = Field(
        ...,
        description="""Name of the attribute after import.
    If not specified, `source` is used.""",
    )
    description: str = Field(
        ...,
        description="""Brief description of the attribute.
        This is required: if you don't know what an attribute means,
        you shouldn't be importing it.""",
    )
    aliases: list[str] = Field(default_factory=list, description="")

    @root_validator(pre=True)
    def target_to_source(cls, vals: dict) -> dict:
        """Assigns `target` to the value of `source` if `target` is not specified."""
        if vals.get("target") is None:
            vals["target"] = vals.get("source")
        return vals


class DataSourceSpecialAttrs(BaseModel):
    """An index of special attributes in a data source."""

    canonical_id: Optional[str] = Field(
        None,
        description="""An attribute used as a unique ID in the source
        (e.g. a Census GEOID).""",
    )
    name: Optional[str] = Field(
        None,
        description="An attribute used as a human-readable name in the source.",
    )
    state_fips: Optional[str] = Field(
        None,
        description="An attribute used as a state FIPS identifier in the source.",
    )
    county_fips: Optional[str] = Field(
        None,
        description="An attribute used as a county FIPS identifier in the source.",
    )


class DataSourceTable(BaseModel):
    """A table of attributes derived from a data source."""

    name: str = Field(
        ...,
        description="The globally unique name of the table.",
    )
    # TODO: more sophisticated wildcard matching here?
    attrs: Union[str, list[str]] = Field(
        ...,
        description="""A list of attributes or attribute aliases to include.
        The special string '*' includes all available attributes in the parent
        data source (with target attribute names).""",
    )
    notes: str = Field(
        ...,
        description="""A description of the table.
        This is required: if you can't describe a table,
        you shouldn't be importing it.""",
    )

    @validator("attrs")
    def wildcard_attrs(cls, value):
        if isinstance(value, str) and value != "*":
            raise ValueError("Specify a wildcard (*) or a list of attributes.")
        return value


class DataSource(BaseModel):
    """A data source (such as a shapefile or Census table)."""

    version: str = Field(
        ...,
        description="""The version descriptor of the data source.
        Typically a year, but could be something more elaborate (e.g. `2020-alt`).""",
    )
    source: HttpUrl = Field(..., description="The source URL of the data.")
    attrs: list[DataSourceAttr] = Field(
        default_factory=list,
        description="""Attributes (columns) in the data source.
        Only explicitly listed columns are imported.
        However, the `geometry` column (if present) is imported implicitly.""",
    )
    special: DataSourceSpecialAttrs = Field(
        default_factory=DataSourceSpecialAttrs,
        description="Special attributes (post-remapping) in the data source.",
    )
    notes: str = Field(
        "",
        description="Notes about the data source's lineage, etc.",
    )
    tables: list[DataSourceTable] = Field(
        default_factory=list,
        description="Tables of attributes derived from the data source.",
    )
    # TODO: `elections`
    # TODO: validate all attributes/aliases in `tables`.

    @root_validator
    def canonical_id_in_attrs(_cls, values):
        canonical_id = values.get("canonical_id")
        attrs = values.get("attrs", [])
        if canonical_id is not None:
            if not any(attr.source == canonical_id for attr in attrs):
                raise ValueError(
                    f'Canonical ID attribute "{canonical_id}" not found in attributes.'
                )
        return values

    def filter_and_rename(self, source_df: pd.DataFrame) -> pd.DataFrame:
        """Filters and renames attributes in a DataFrame based on `attrs`."""
        attr_subset = {attr.source: attr.target for attr in self.attrs}
        if "geometry" in source_df.columns:
            attr_subset["geometry"] = "geometry"
        return source_df[list(attr_subset)].rename(columns=attr_subset)


def config_logger(logger: logging.Logger) -> None:
    """Configures a logger to write to `stderr`."""
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
