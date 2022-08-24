"""SQL table definitions for CherryDB."""
from enum import Enum
from sqlalchemy import (
    Enum as SqlEnum,
    ForeignKey,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Boolean,
    MetaData,
    UniqueConstraint,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects import postgresql
from geoalchemy2 import Geometry
from cherrydb_meta.schemas import GeoUnit

DEFAULT_LENGTH = 200

metadata_obj = MetaData(schema="cherrydb")
Base = declarative_base(metadata=metadata_obj)


class User(Base):
    __tablename__ = "user"

    user_id = Column(Integer, primary_key=True)
    name = Column(String(DEFAULT_LENGTH), nullable=False)
    email = Column(String(254), nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Geography(Base):
    __tablename__ = "geography"
    __table_args__ = UniqueConstraint("name", "unit", "vintage")

    geo_id = Column(Integer, primary_key=True)
    name = Column(String(DEFAULT_LENGTH), nullable=False)
    unit = Column(SqlEnum(GeoUnit), nullable=False)
    vintage = Column(Integer, nullable=False)
    default_proj = Column(String(DEFAULT_LENGTH))

    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("user.user_id"), nullable=False)


class GeoAlias(Base):
    __tablename__ = "geo_alias"

    alias_id = Column(Integer, primary_key=True)
    geo_id = Column(Integer, ForeignKey("geography.geo_id"), nullable=False)

    alias = Column(String(DEFAULT_LENGTH), nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(
        "created_by", Integer, ForeignKey("user.user_id"), nullable=False
    )


class GeoNode(Base):
    __tablename__ = "geo_node"

    node_id = Column(Integer, primary_key=True)
    geo_id = Column(Integer, ForeignKey("geography.geo_id"), nullable=False)
    canonical_id = Column(String(DEFAULT_LENGTH), nullable=False)
    geometry = Column(Geometry, nullable=False)


class GovLevel(str, Enum):
    STATE_HOUSE = "state_house"
    STATE_SENATE = "state_senate"
    US_HOUSE = "us_house"
    OTHER = "other"


class Ensemble(Base):
    __tablename__ = "ensemble"

    ensemble_id = Column(Integer, primary_key=True)
    graph_id = Column(Integer, ForeignKey("dual_graph.graph_id"), nullable=False)
    path = Column(String(4096), nullable=False, unique=True)
    level = Column(SqlEnum(GovLevel), nullable=False)
    count = Column(Integer, nullable=False)
    count_distinct = Column(Integer, nullable=False)

    label = Column(String(DEFAULT_LENGTH), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("user.user_id"), nullable=False)


class Plan(Base):
    __tablename__ = "plan"

    plan_id = Column(Integer, primary_key=True)
    geo_id = Column(Integer, ForeignKey("geography.geo_id"), nullable=False)
    level = Column(SqlEnum(GovLevel), nullable=False)
    assignment = Column(postgresql.ARRAY(Integer, dimensions=1, zero_indexes=True))

    label = Column(String(DEFAULT_LENGTH), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("user.user_id"), nullable=False)


class DualGraph(Base):
    __tablename__ = "dual_graph"

    graph_id = Column(Integer, primary_key=True)
    geo_id = Column(Integer, ForeignKey("geography.geo_id"), nullable=False)
    variant = Column(String(DEFAULT_LENGTH), default="default", nullable=False)
    node_ids = Column(postgresql.ARRAY(Integer, dimensions=1, zero_indexes=True))
    edges = Column(postgresql.ARRAY(Integer, dimensions=2, zero_indexes=True))

    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("user.user_id"), nullable=False)


class GeoAttrType(str, Enum):
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STR = "str"


class GeoAttr(Base):
    __tablename__ = "geo_attr"

    attr_id = Column(Integer, primary_key=True)
    name = Column(String(DEFAULT_LENGTH), nullable=False)
    vintage = Column(Integer, nullable=False)
    type = Column(SqlEnum(GeoAttrType), nullable=False)

    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("user.user_id"), nullable=False)


class GeoAttrFloat(Base):
    __tablename__ = "geo_attr_float"

    node_id = Column(
        Integer, ForeignKey("geo_node.node_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("geo_attr.attr_id"), nullable=False, primary_key=True
    )
    val = Column(postgresql.DOUBLE_PRECISION, nullable=False)


class GeoAttrInt(Base):
    __tablename__ = "geo_attr_int"

    node_id = Column(
        Integer, ForeignKey("geo_node.node_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("geo_attr.attr_id"), nullable=False, primary_key=True
    )
    val = Column(Integer, nullable=False)


class GeoAttrBool(Base):
    __tablename__ = "geo_attr_bool"

    node_id = Column(
        Integer, ForeignKey("geo_node.node_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("geo_attr.attr_id"), nullable=False, primary_key=True
    )
    val = Column(Boolean, nullable=False)


class GeoAttrStr(Base):
    __tablename__ = "geo_attr_str"

    node_id = Column(
        Integer, ForeignKey("geo_node.node_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("geo_attr.attr_id"), nullable=False, primary_key=True
    )
    val = Column(String(65535), nullable=False)
