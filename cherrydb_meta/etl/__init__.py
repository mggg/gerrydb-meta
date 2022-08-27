"""Common ETL functionality and data models."""
import logging
import os
import sys
from contextlib import contextmanager
from typing import Generator, Optional, Union

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, HttpUrl, root_validator, validator
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session as SessionType
from sqlalchemy.orm import sessionmaker

from cherrydb_meta.models import (
    AliasKind,
    Location,
    LocationAlias,
    LocationKind,
    ObjectMeta,
    User,
)

log = logging.getLogger()


class BaseModel(PydanticBaseModel):
    """Base model for ETL configuration."""

    class Config:
        allow_mutation = False
        arbitrary_types_allowed = True


class ETLUpserter(BaseModel):
    """Wraps ETL upsert operations."""

    session: SessionType
    user: User

    def location(
        self,
        name: str,
        kind: LocationKind,
        aliases: list[tuple[str, AliasKind]],
        parent: Optional[Location] = None,
        notes: Optional[str] = None,
    ) -> Location:
        """Upserts a location (with aliases)."""
        meta = ObjectMeta(created_by=self.user.user_id, notes=notes)
        self.session.add(meta)
        self.session.flush()
        self.session.refresh(meta)

        log.info("Upserting location (name = %s, kind = %s)...", name, kind)
        loc_stmt = (
            insert(Location)
            .values(
                name=name,
                kind=kind,
                parent_id=parent.loc_id if parent else None,
                meta_id=meta.meta_id,
            )
            .on_conflict_do_nothing()
        )
        self.session.execute(loc_stmt)
        loc = self.session.query(Location).filter(Location.name == name).first()
        log.info("Upserted %s.", loc)

        alias_values = [
            {"name": name, "kind": kind, "loc_id": loc.loc_id, "meta_id": meta.meta_id}
            for name, kind in aliases
        ]
        alias_stmt = insert(LocationAlias).values(alias_values).on_conflict_do_nothing()
        self.session.execute(alias_stmt)
        log.info("Upserted %d aliases for %s.", len(aliases), loc)
        return loc


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
    def upserter(self) -> Generator[SessionType, None, None]:
        """Yields an upserter, which contains a database session.
        
        Commits the outstanding transaction if no errors exceptions
        occur in the context manager scope;
        otherwise, rolls back the outstanding transaction.

        Raises:
            ValueError: No `user` is available.
        """
        if self.user is None:
            raise ValueError("Cannot create an upserter without an ETL user.")
        with self.session() as session:
            yield ETLUpserter(session=session, user=self.user)

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


class DataSourceTable(BaseModel):
    """A table of attributes derived from a data source."""

    name: str = Field(
        ..., description="The globally unique name of the table.",
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
    canonical_id: Optional[str] = Field(
        None,
        description="""An attribute used as a unique ID in the original source
        (e.g.  a Census GEOID).""",
    )
    attrs: list[DataSourceAttr] = Field(
        default_factory=list,
        description="""Attributes (columns) in the data source.
        Only explicitly listed columns are imported.
        However, the `geometry` column (if present) is imported implicitly.""",
    )
    notes: str = Field(
        "", description="Notes about the data source's lineage, etc.",
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


def config_logger(logger: logging.Logger) -> None:
    """Configures a logger to write to `stderr`."""
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
