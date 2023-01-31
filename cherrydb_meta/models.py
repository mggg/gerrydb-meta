"""SQL table definitions for CherryDB."""
from geoalchemy2 import Geometry
from sqlalchemy import JSON, Boolean, CheckConstraint, Column, DateTime
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import (
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship
from sqlalchemy.sql import func
from cherrydb_meta.enums import ColumnType, ScopeType, NamespaceGroup

metadata_obj = MetaData(schema="cherrydb")


class Base(DeclarativeBase):
    metadata = metadata_obj


class User(Base):
    __tablename__ = "user"

    user_id = mapped_column(Integer, primary_key=True)
    name = mapped_column(Text, nullable=False)
    email = mapped_column(String(254), nullable=False, unique=True, index=True)
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())

    scopes = relationship("UserScope", lazy="joined")
    groups = relationship("UserGroupMember", lazy="joined")
    api_keys = relationship("ApiKey", back_populates="user")

    def __str__(self):
        return f"User(email={self.email}, name={self.name})"


class UserGroup(Base):
    __tablename__ = "user_group"

    group_id = mapped_column(Integer, primary_key=True)
    name = mapped_column(Text, nullable=False)
    description = mapped_column(Text, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    scopes = relationship("UserGroupScope", lazy="joined")
    users = relationship("UserGroupMember", lazy="joined", back_populates="group")
    meta = relationship("ObjectMeta")

    def __str__(self):
        return f"UserGroup(name={self.name})"


class UserGroupMember(Base):
    __tablename__ = "user_group_member"

    user_id = mapped_column(Integer, ForeignKey("user.user_id"), primary_key=True)
    group_id = mapped_column(
        Integer, ForeignKey("user_group.group_id"), primary_key=True
    )
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    user = relationship("User", lazy="joined", back_populates="groups")
    group = relationship("UserGroup", lazy="joined", back_populates="users")
    meta = relationship("ObjectMeta")


class UserScope(Base):
    __tablename__ = "user_scope"
    __table_args__ = (UniqueConstraint("user_id", "scope", "scope", "namespace_id"),)

    user_perm_id = mapped_column(Integer, primary_key=True)
    user_id = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False)
    scope = mapped_column(SqlEnum(ScopeType), nullable=False)
    namespace_group = mapped_column(SqlEnum(NamespaceGroup))
    namespace_id = mapped_column(Integer, ForeignKey("namespace.namespace_id"))
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    user = relationship("User", back_populates="scopes")
    namespace = relationship("Namespace")
    meta = relationship("ObjectMeta")


class UserGroupScope(Base):
    __tablename__ = "user_group_scope"
    __table_args__ = (UniqueConstraint("group_id", "scope", "scope", "namespace_id"),)

    group_perm_id = mapped_column(Integer, primary_key=True)
    group_id = mapped_column(Integer, ForeignKey("user_group.group_id"), nullable=False)
    scope = mapped_column(SqlEnum(ScopeType), nullable=False)
    namespace_group = mapped_column(SqlEnum(NamespaceGroup))
    namespace_id = mapped_column(Integer, ForeignKey("namespace.namespace_id"))
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    group = relationship("UserGroup", back_populates="scopes")
    namespace = relationship("Namespace")
    meta = relationship("ObjectMeta")


class ApiKey(Base):
    __tablename__ = "api_key"

    key_hash = mapped_column(LargeBinary, primary_key=True)
    user_id = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False)
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    active = mapped_column(Boolean, default=True)

    user = relationship("User", back_populates="api_keys")


class ObjectMeta(Base):
    __tablename__ = "meta"

    meta_id = mapped_column(Integer, primary_key=True)
    notes = mapped_column(Text)
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_by = mapped_column(Integer, ForeignKey("user.user_id"), nullable=False)

    user = relationship("User")


class Namespace(Base):
    __tablename__ = "namespace"

    namespace_id = mapped_column(Integer, primary_key=True)
    path = mapped_column(Text, nullable=False, unique=True, index=True)
    name = mapped_column(Text, nullable=False, unique=True)
    description = mapped_column(Text, nullable=False)
    public = mapped_column(Boolean, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")


class Locality(Base):
    __tablename__ = "locality"
    __table_args__ = (CheckConstraint("parent_id <> loc_id"),)

    loc_id = mapped_column(Integer, primary_key=True)
    canonical_ref_id = mapped_column(
        Integer,
        ForeignKey("locality_ref.ref_id"),
        unique=True,
        index=True,
        nullable=False,
    )
    parent_id = mapped_column(Integer, ForeignKey("locality.loc_id"))
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)
    name = mapped_column(Text, nullable=False)

    parent = relationship("Locality", remote_side=[loc_id])
    meta = relationship("ObjectMeta", lazy="joined")
    canonical_ref = relationship(
        "LocalityRef",
        lazy="joined",
        primaryjoin="Locality.canonical_ref_id==LocalityRef.ref_id",
    )
    refs = relationship(
        "LocalityRef", primaryjoin="Locality.loc_id==LocalityRef.loc_id"
    )

    def __str__(self):
        return f"Locality(loc_id={self.loc_id}, name={self.name})"


class LocalityRef(Base):
    __tablename__ = "locality_ref"

    ref_id = mapped_column(Integer, primary_key=True)
    loc_id = mapped_column(Integer, ForeignKey("locality.loc_id"))
    path = mapped_column(Text, unique=True, index=True, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    loc = relationship(
        "Locality",
        lazy="joined",
        primaryjoin="Locality.loc_id==LocalityRef.loc_id",
        overlaps="refs",
    )


class GeoSet(Base):
    __tablename__ = "geo_set"
    __table_args__ = (UniqueConstraint("name", "namespace_id"),)

    set_id = mapped_column(Integer, primary_key=True)
    loc_id = mapped_column(Integer, ForeignKey("locality.loc_id"), nullable=False)
    name = mapped_column(Text, nullable=False)
    namespace_id = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    srid = mapped_column(Integer)
    description = mapped_column(Text)
    source_url = mapped_column(String(2048))
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")


class Geography(Base):
    __tablename__ = "geography"

    geo_id = mapped_column(Integer, primary_key=True)
    name = mapped_column(Text, nullable=False)
    geometry = mapped_column(Geometry, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class GeoMember(Base):
    __tablename__ = "geo_member"

    geo_id = mapped_column(Integer, ForeignKey("geo_set.set_id"), primary_key=True)
    node_id = mapped_column(Integer, ForeignKey("geography.geo_id"), primary_key=True)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class GeoHierarchy(Base):
    __tablename__ = "geo_hierarchy"
    __table_args__ = (CheckConstraint("parent_id <> child_id"),)

    parent_id = mapped_column(Integer, ForeignKey("geography.geo_id"), primary_key=True)
    child_id = mapped_column(Integer, ForeignKey("geography.geo_id"), primary_key=True)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class DataColumn(Base):
    __tablename__ = "column"

    col_id = mapped_column(Integer, primary_key=True)
    namespace_id = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    canonical_ref_id = mapped_column(
        Integer,
        ForeignKey("column_ref.ref_id"),
        unique=True,
        index=True,
        nullable=False,
    )
    description = mapped_column(Text)
    type = mapped_column(SqlEnum(ColumnType), nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")
    namespace = relationship("Namespace", lazy="joined")


class ColumnRef(Base):
    __tablename__ = "column_ref"
    __table_args__ = (UniqueConstraint("namespace_id", "path"),)

    ref_id = mapped_column(Integer, primary_key=True)
    namespace_id = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    col_id = mapped_column(Integer, ForeignKey("column.col_id"))
    path = mapped_column(Text, index=True, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    column = relationship(
        "DataColumn",
        lazy="joined",
        primaryjoin="DataColumn.col_id==ColumnRef.col_id",
        overlaps="refs",
    )


class ColumnRelation(Base):
    __tablename__ = "column_relation"
    __table_args__ = (UniqueConstraint("namespace_id", "name"),)

    relation_id = mapped_column(Integer, primary_key=True)
    namespace_id = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    name = mapped_column(Text, nullable=False)
    expr = mapped_column(JSON, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")


class ColumnRelationMember(Base):
    __tablename__ = "column_relation_member"

    relation_id = mapped_column(
        Integer, ForeignKey("column_relation.relation_id"), primary_key=True
    )
    member_id = mapped_column(Integer, ForeignKey("column.col_id"), primary_key=True)


class ColumnSet(Base):
    __tablename__ = "column_set"
    __table_args__ = (UniqueConstraint("name", "namespace_id"),)

    set_id = mapped_column(Integer, primary_key=True)
    name = mapped_column(Text, nullable=False)
    namespace_id = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    description = mapped_column(Text)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta", lazy="joined")


class ColumnSetMember(Base):
    __tablename__ = "column_set_member"

    set_id = mapped_column(Integer, ForeignKey("column_set.set_id"), primary_key=True)
    col_id = mapped_column(Integer, ForeignKey("column.col_id"), primary_key=True)


class ColumnValueFloat(Base):
    __tablename__ = "column_value_float"

    node_id = mapped_column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    attr_id = mapped_column(
        Integer, ForeignKey("column.col_id"), nullable=False, primary_key=True
    )
    val = mapped_column(postgresql.DOUBLE_PRECISION, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class ColumnValueInt(Base):
    __tablename__ = "column_value_int"

    node_id = mapped_column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    attr_id = mapped_column(
        Integer, ForeignKey("column.col_id"), nullable=False, primary_key=True
    )
    val = mapped_column(Integer, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class ColumnValueBool(Base):
    __tablename__ = "column_value_bool"

    node_id = mapped_column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    attr_id = mapped_column(
        Integer, ForeignKey("column.col_id"), nullable=False, primary_key=True
    )
    val = mapped_column(Boolean, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class ColumnValueStr(Base):
    __tablename__ = "column_value_str"

    node_id = mapped_column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    attr_id = mapped_column(
        Integer, ForeignKey("column.col_id"), nullable=False, primary_key=True
    )
    val = mapped_column(String(65535), nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")


class ColumnValueJSON(Base):
    __tablename__ = "column_value_json"

    node_id = mapped_column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    attr_id = mapped_column(
        Integer, ForeignKey("column.col_id"), nullable=False, primary_key=True
    )
    val = mapped_column(postgresql.JSONB, nullable=False)
    meta_id = mapped_column(Integer, ForeignKey("meta.meta_id"), nullable=False)

    meta = relationship("ObjectMeta")
