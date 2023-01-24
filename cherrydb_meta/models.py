"""SQL table definitions for CherryDB."""
from enum import Enum

from geoalchemy2 import Geometry
from sqlalchemy import Boolean, CheckConstraint, Column, DateTime
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import ForeignKey, Integer, MetaData, String, Text, UniqueConstraint, LargeBinary, JSON, REAL
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import func

metadata_obj = MetaData(schema="cherrydb")
Base = declarative_base(metadata=metadata_obj)

class ColumnType(str, Enum):
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STR = "str"


class User(Base):
    __tablename__ = "user"

    user_id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    email = Column(String(254), nullable=False, unique=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    api_keys = relationship("ApiKey", back_populates="user")

    def __str__(self):
        return f"User(email={self.email}, name={self.name})"


class ApiKey(Base):
    __tablename__ = "api_key"

    key_hash = Column(LargeBinary, primary_key=True)
    user_id = Column(Integer, ForeignKey("user.user_id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    active = Column(Boolean, default=True)

    user = relationship("User", back_populates="api_keys")


class ObjectMeta(Base):
    __tablename__ = "meta"

    meta_id = Column(Integer, primary_key=True)
    notes = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("user.user_id"), nullable=False)

    user = relationship("User")


class Namespace(Base):
    __tablename__ = "namespace"

    namespace_id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False, unique=True)
    description = Column(Text)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")


class Location(Base):
    __tablename__ = "location"
    __table_args__ = (CheckConstraint("parent_id <> loc_id"),)

    loc_id = Column(Integer, primary_key=True)
    canonical_ref_id = Column(
        Integer,
        ForeignKey("location_ref.ref_id"),
        unique=True,
        index=True,
        nullable=False,
    )
    parent_id = Column(Integer, ForeignKey("location.loc_id"))
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)
    name = Column(Text, nullable=False)

    parent = relationship("Location")
    meta = relationship("ObjectMeta", lazy="joined")
    canonical_ref = relationship(
        "LocationRef",
        lazy="joined",
        primaryjoin="Location.canonical_ref_id==LocationRef.ref_id",
    )
    refs = relationship(
        "LocationRef", primaryjoin="Location.loc_id==LocationRef.loc_id"
    )

    def __str__(self):
        return f"Location(loc_id={self.loc_id}, name={self.name})"


class LocationRef(Base):
    __tablename__ = "location_ref"

    ref_id = Column(Integer, primary_key=True)
    loc_id = Column(Integer, ForeignKey("location.loc_id"))
    path = Column(Text, unique=True, index=True, nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    loc = relationship(
        "Location",
        lazy="joined",
        primaryjoin="Location.loc_id==LocationRef.loc_id",
        overlaps="refs",
    )


class GeoSet(Base):
    __tablename__ = "geo_set"
    __table_args__ = (UniqueConstraint("name", "namespace_id"),)

    set_id = Column(Integer, primary_key=True)
    loc_id = Column(Integer, ForeignKey("location.loc_id"), nullable=False)
    name = Column(Text, nullable=False)
    namespace_id = Column(Integer, ForeignKey("namespace.namespace_id"), nullable=False)
    srid = Column(Integer)
    description = Column(Text)
    source_url = Column(String(2048))
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")


class Geography(Base):
    __tablename__ = "geography"

    geo_id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    geometry = Column(Geometry, nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class GeoMember(Base):
    __tablename__ = "geo_member"

    geo_id = Column(Integer, ForeignKey("geo_set.set_id"), primary_key=True)
    node_id = Column(Integer, ForeignKey("geography.geo_id"), primary_key=True)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class GeoHierarchy(Base):
    __tablename__ = "geo_hierarchy"
    __table_args__ = (CheckConstraint("parent_id <> child_id"),)

    parent_id = Column(Integer, ForeignKey("geography.geo_id"), primary_key=True)
    child_id = Column(Integer, ForeignKey("geography.geo_id"), primary_key=True)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class DataColumn(Base):
    __tablename__ = "column"
    __table_args__ = (UniqueConstraint("namespace_id", "name"),)

    col_id = Column(Integer, primary_key=True)
    namespace_id = Column(Integer, ForeignKey("namespace.namespace_id"), nullable=False)
    name = Column(Text, nullable=False)
    description = Column(Text)
    type = Column(SqlEnum(ColumnType), nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")


class ColumnRelation(Base):
    __tablename__ = "column_relation"
    __table_args__ = (UniqueConstraint("namespace_id", "name"),)

    relation_id = Column(Integer, primary_key=True)
    namespace_id = Column(Integer, ForeignKey("namespace.namespace_id"), nullable=False)
    name = Column(Text, nullable=False)
    expr = Column(JSON, nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")


class ColumnRelationMember(Base):
    __tablename__ = "column_relation_member"

    relation_id = Column(
        Integer, ForeignKey("column_relation.relation_id"), primary_key=True
    )
    member_id = Column(Integer, ForeignKey("column.col_id"), primary_key=True)


class ColumnSet(Base):
    __tablename__ = "column_set"
    __table_args__ = (UniqueConstraint("name", "namespace_id"),)

    set_id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    namespace_id = Column(Integer, ForeignKey("namespace.namespace_id"), nullable=False)
    description = Column(Text)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")


class ColumnSetMember(Base):
    __tablename__ = "column_set_member"

    set_id = Column(Integer, ForeignKey("column_set.set_id"), primary_key=True)
    col_id = Column(Integer, ForeignKey("column.col_id"), primary_key=True)


class ColumnValueFloat(Base):
    __tablename__ = "column_value_float"

    node_id = Column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("column.col_id"), nullable=False, primary_key=True
    )
    val = Column(postgresql.DOUBLE_PRECISION, nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class ColumnValueInt(Base):
    __tablename__ = "column_value_int"

    node_id = Column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("column.col_id"), nullable=False, primary_key=True
    )
    val = Column(Integer, nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class ColumnValueBool(Base):
    __tablename__ = "column_value_bool"

    node_id = Column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("column.col_id"), nullable=False, primary_key=True
    )
    val = Column(Boolean, nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class ColumnValueStr(Base):
    __tablename__ = "column_value_str"

    node_id = Column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    attr_id = Column(
        Integer, ForeignKey("column.col_id"), nullable=False, primary_key=True
    )
    val = Column(String(65535), nullable=False)
    meta_id = Column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")
