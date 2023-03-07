"""SQL table definitions for CherryDB."""
import uuid
from datetime import datetime
from typing import Any

from geoalchemy2 import Geography as SqlGeography
from sqlalchemy import JSON, BigInteger, Boolean, CheckConstraint, DateTime
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
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    column_property,
    mapped_column,
    relationship,
)
from sqlalchemy.sql import func

from cherrydb_meta.enums import ColumnKind, ColumnType, NamespaceGroup, ScopeType

metadata_obj = MetaData(schema="cherrydb")


class Base(DeclarativeBase):
    metadata = metadata_obj


class User(Base):
    __tablename__ = "user"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(
        String(254), nullable=False, unique=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    scopes: Mapped[list["UserScope"]] = relationship("UserScope", lazy="joined")
    groups: Mapped[list["UserGroupMember"]] = relationship(
        "UserGroupMember", lazy="joined"
    )
    api_keys: Mapped["ApiKey"] = relationship("ApiKey", back_populates="user")

    def __repr__(self):
        return f"User(email={self.email}, name={self.name})"


class UserGroup(Base):
    __tablename__ = "user_group"

    group_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    scopes: Mapped[list["UserGroupScope"]] = relationship(
        "UserGroupScope", lazy="joined", uselist=True
    )
    users: Mapped[list["UserGroupMember"]] = relationship(
        "UserGroupMember", lazy="joined", back_populates="group"
    )
    meta: Mapped["ObjectMeta"] = relationship("ObjectMeta")

    def __repr__(self):
        return f"UserGroup(name={self.name})"


class UserGroupMember(Base):
    __tablename__ = "user_group_member"

    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("user.user_id"), primary_key=True
    )
    group_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("user_group.group_id"), primary_key=True
    )
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    user: Mapped[User] = relationship("User", lazy="joined", back_populates="groups")
    group = relationship("UserGroup", lazy="joined", back_populates="users")
    meta: Mapped["ObjectMeta"] = relationship("ObjectMeta")


class UserScope(Base):
    __tablename__ = "user_scope"
    __table_args__ = (UniqueConstraint("user_id", "scope", "scope", "namespace_id"),)

    user_perm_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("user.user_id"), nullable=False
    )
    scope: Mapped[ScopeType] = mapped_column(SqlEnum(ScopeType), nullable=False)
    namespace_group: Mapped[NamespaceGroup | None] = mapped_column(
        SqlEnum(NamespaceGroup)
    )
    namespace_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id")
    )
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    user: Mapped[User] = relationship("User", back_populates="scopes")
    namespace: Mapped["Namespace"] = relationship("Namespace")
    meta: Mapped["ObjectMeta"] = relationship("ObjectMeta")

    def __repr__(self):
        return (
            f"UserScope(user_id={self.user_id}, scope={self.scope}, "
            f"namespace_group={self.namespace_group}, "
            f"namespace_id={self.namespace_id})"
        )


class UserGroupScope(Base):
    __tablename__ = "user_group_scope"
    __table_args__ = (UniqueConstraint("group_id", "scope", "scope", "namespace_id"),)

    group_perm_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    group_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("user_group.group_id"), nullable=False
    )
    scope: Mapped[ScopeType] = mapped_column(SqlEnum(ScopeType), nullable=False)
    namespace_group: Mapped[NamespaceGroup | None] = mapped_column(
        SqlEnum(NamespaceGroup)
    )
    namespace_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id")
    )
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    group: Mapped[UserGroup] = relationship("UserGroup", back_populates="scopes")
    namespace: Mapped["Namespace"] = relationship("Namespace")
    meta: Mapped["ObjectMeta"] = relationship("ObjectMeta")


class ApiKey(Base):
    __tablename__ = "api_key"

    key_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("user.user_id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    user: Mapped[User] = relationship("User", back_populates="api_keys")


class ObjectMeta(Base):
    __tablename__ = "meta"

    meta_id = mapped_column(Integer, primary_key=True)
    uuid = mapped_column(
        postgresql.UUID(as_uuid=True),
        index=True,
        unique=True,
        nullable=False,
        default=uuid.uuid4,
    )
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[int] = mapped_column(
        Integer, ForeignKey("user.user_id"), nullable=False
    )

    user: Mapped[User] = relationship("User")


class Namespace(Base):
    __tablename__ = "namespace"

    namespace_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    path: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    public: Mapped[bool] = mapped_column(Boolean, nullable=False)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")


class Locality(Base):
    __tablename__ = "locality"
    __table_args__ = (CheckConstraint("parent_id <> loc_id"),)

    loc_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    canonical_ref_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("locality_ref.ref_id"),
        unique=True,
        index=True,
        nullable=False,
    )
    parent_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("locality.loc_id")
    )
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    default_proj: Mapped[str | None] = mapped_column(Text)

    parent = relationship("Locality", remote_side=[loc_id])
    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    canonical_ref: Mapped["LocalityRef"] = relationship(
        "LocalityRef",
        lazy="joined",
        primaryjoin="Locality.canonical_ref_id==LocalityRef.ref_id",
    )
    refs: Mapped[list["LocalityRef"]] = relationship(
        "LocalityRef", primaryjoin="Locality.loc_id==LocalityRef.loc_id"
    )

    def __str__(self):
        return f"Locality(loc_id={self.loc_id}, name={self.name})"


class LocalityRef(Base):
    __tablename__ = "locality_ref"

    ref_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    loc_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("locality.loc_id"))
    path: Mapped[str] = mapped_column(Text, unique=True, index=True, nullable=False)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    loc: Mapped[Locality] = relationship(
        "Locality",
        lazy="joined",
        primaryjoin="Locality.loc_id==LocalityRef.loc_id",
        overlaps="refs",
    )


class GeoLayer(Base):
    __tablename__ = "geo_layer"
    __table_args__ = (UniqueConstraint("path", "namespace_id"),)

    layer_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    description: Mapped[str | None] = mapped_column(Text)
    source_url: Mapped[str | None] = mapped_column(String(2048))
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")


class GeoSetVersion(Base):
    __tablename__ = "geo_set_version"

    set_version_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    layer_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geo_layer.layer_id"), nullable=False
    )
    loc_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("locality.loc_id"), nullable=False
    )
    valid_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    valid_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    layer: Mapped[GeoLayer] = relationship("GeoLayer", lazy="joined")
    loc: Mapped[Locality] = relationship("Locality", lazy="joined")
    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    members: Mapped[list["GeoSetMember"]] = relationship("GeoSetMember")


class GeoSetMember(Base):
    __tablename__ = "geo_set_member"

    set_version_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geo_set_version.set_version_id"), primary_key=True
    )
    geo_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geography.geo_id"), primary_key=True
    )

    set_version: Mapped[GeoSetVersion] = relationship(
        "GeoSetVersion", back_populates="members"
    )


class GeoVersion(Base):
    __tablename__ = "geo_version"

    import_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geo_import.import_id"), primary_key=True
    )
    geo_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geography.geo_id"), primary_key=True
    )
    valid_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    valid_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    geography = mapped_column(SqlGeography(srid=4326), nullable=False)
    internal_point = mapped_column(SqlGeography(geometry_type="POINT", srid=4326))

    parent: Mapped["Geography"] = relationship(
        "Geography", back_populates="versions", lazy="joined"
    )


class Geography(Base):
    __tablename__ = "geography"

    geo_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")
    versions: Mapped[list[GeoVersion]] = relationship("GeoVersion")

    @property
    def full_path(self):
        """Path with namespace prefix."""
        return f"/{self.namespace.path}/{self.path}"


class GeoImport(Base):
    __tablename__ = "geo_import"

    import_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid = mapped_column(
        postgresql.UUID(as_uuid=True),
        index=True,
        unique=True,
        nullable=False,
        default=uuid.uuid4,
    )
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[int] = mapped_column(
        Integer, ForeignKey("user.user_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")
    user: Mapped[User] = relationship("User", lazy="joined")


# TODO: should these be versioned? tagged by algorithm?
class GeoHierarchy(Base):
    __tablename__ = "geo_hierarchy"
    __table_args__ = (CheckConstraint("parent_id <> child_id"),)

    parent_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geography.geo_id"), primary_key=True
    )
    child_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geography.geo_id"), primary_key=True
    )
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta")


class DataColumn(Base):
    __tablename__ = "column"

    col_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    canonical_ref_id = mapped_column(
        Integer,
        ForeignKey("column_ref.ref_id"),
        unique=True,
        index=True,
        nullable=False,
    )
    description: Mapped[str | None] = mapped_column(Text)
    source_url: Mapped[str | None] = mapped_column(String(2048))
    kind: Mapped[ColumnKind] = mapped_column(SqlEnum(ColumnKind), nullable=False)
    type: Mapped[ColumnType] = mapped_column(SqlEnum(ColumnType), nullable=False)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")
    canonical_ref = relationship(
        "ColumnRef",
        lazy="joined",
        primaryjoin="DataColumn.canonical_ref_id==ColumnRef.ref_id",
    )
    refs: Mapped[list["ColumnRef"]] = relationship(
        "ColumnRef", primaryjoin="DataColumn.col_id==ColumnRef.col_id"
    )


class ColumnRef(Base):
    __tablename__ = "column_ref"
    __table_args__ = (UniqueConstraint("namespace_id", "path"),)

    ref_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    col_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("column.col_id"))
    path: Mapped[str] = mapped_column(Text, index=True, nullable=False)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")
    column: Mapped[DataColumn] = relationship(
        "DataColumn",
        lazy="joined",
        primaryjoin="DataColumn.col_id==ColumnRef.col_id",
        overlaps="refs",
    )

    @property
    def full_path(self):
        """Path with namespace prefix."""
        return f"/{self.namespace.path}/{self.path}"


class ColumnRelation(Base):
    __tablename__ = "column_relation"
    __table_args__ = (UniqueConstraint("namespace_id", "name"),)

    relation_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    expr: Mapped[Any] = mapped_column(JSON, nullable=False)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")


class ColumnRelationMember(Base):
    __tablename__ = "column_relation_member"

    relation_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("column_relation.relation_id"), primary_key=True
    )
    member_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("column.col_id"), primary_key=True
    )


class ColumnSet(Base):
    __tablename__ = "column_set"
    __table_args__ = (UniqueConstraint("path", "namespace_id"),)

    set_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False
    )
    description: Mapped[str | None] = mapped_column(Text)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    columns: Mapped[list["ColumnSetMember"]] = relationship(
        "ColumnSetMember", lazy="joined"
    )
    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")


class ColumnSetMember(Base):
    __tablename__ = "column_set_member"

    set_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("column_set.set_id"), primary_key=True
    )
    ref_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("column_ref.ref_id"), primary_key=True
    )
    order: Mapped[int] = mapped_column(Integer, nullable=False)

    set: Mapped[ColumnSet] = relationship("ColumnSet", back_populates="columns")
    ref: Mapped[ColumnRef] = relationship("ColumnRef", lazy="joined")


class ColumnValue(Base):
    __tablename__ = "column_value"

    col_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("column.col_id"),
        nullable=False,
        primary_key=True,
    )
    geo_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geography.geo_id"), nullable=False, primary_key=True
    )
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )
    valid_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    valid_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    val_float: Mapped[float] = mapped_column(postgresql.DOUBLE_PRECISION, nullable=True)
    val_int: Mapped[int] = mapped_column(BigInteger, nullable=True)
    val_str: Mapped[str] = mapped_column(Text, nullable=True)
    val_bool: Mapped[bool] = mapped_column(Boolean, nullable=True)
    val_json: Mapped[Any] = mapped_column(postgresql.JSONB, nullable=True)

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta")


class ViewTemplate(Base):
    __tablename__ = "view_template"
    __table_args__ = (UniqueConstraint("namespace_id", "path"),)

    template_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False, index=True
    )
    path: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")


class ViewTemplateVersion(Base):
    __tablename__ = "view_template_version"

    template_version_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    template_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("view_template.template_id"), nullable=False
    )
    valid_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    valid_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    parent: Mapped[ViewTemplate] = relationship("ViewTemplate", lazy="joined")

    columns: Mapped[list["ViewTemplateColumnMember"]] = relationship(
        "ViewTemplateColumnMember", lazy="joined"
    )
    column_sets: Mapped[list["ViewTemplateColumnSetMember"]] = relationship(
        "ViewTemplateColumnSetMember", lazy="joined"
    )


class ViewTemplateColumnMember(Base):
    __tablename__ = "view_template_column_member"

    template_version_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("view_template_version.template_version_id"),
        primary_key=True,
    )
    ref_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("column_ref.ref_id"), primary_key=True
    )
    order: Mapped[int] = mapped_column(Integer, nullable=False)

    template_version: Mapped[ViewTemplate] = relationship(
        "ViewTemplateVersion", back_populates="columns"
    )
    member: Mapped[ColumnRef] = relationship("ColumnRef", lazy="joined")


class ViewTemplateColumnSetMember(Base):
    __tablename__ = "view_template_column_set_member"

    template_version_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("view_template_version.template_version_id"),
        primary_key=True,
    )
    set_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("column_set.set_id"), primary_key=True
    )
    order: Mapped[int] = mapped_column(Integer, nullable=False)

    template_version: Mapped[ViewTemplate] = relationship(
        "ViewTemplateVersion", back_populates="column_sets"
    )
    member: Mapped[ColumnSet] = relationship("ColumnSet", lazy="joined")


class View(Base):
    __tablename__ = "view"
    __table_args__ = (UniqueConstraint("namespace_id", "path"),)

    view_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False, index=True
    )
    path: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    template_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("view_template.template_id"), nullable=False
    )
    # Technically redundant with (template_id, at), but quite useful.
    template_version_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("view_template_version.template_version_id"), nullable=False
    )
    loc_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("locality.loc_id"), nullable=False
    )
    layer_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geo_layer.layer_id"), nullable=False
    )
    at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    proj: Mapped[str | None] = mapped_column(Text)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )

    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")
    template: Mapped[ViewTemplate] = relationship("ViewTemplate", lazy="joined")
    template_version: Mapped[ViewTemplateVersion] = relationship(
        "ViewTemplateVersion", lazy="joined"
    )
    loc: Mapped[Locality] = relationship("Locality", lazy="joined")
    layer: Mapped[GeoLayer] = relationship("GeoLayer", lazy="joined")
    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")


class ETag(Base):
    __tablename__ = "etag"
    __table_args__ = (UniqueConstraint("namespace_id", "table"),)

    etag_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    namespace_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id")
    )
    table: Mapped[str] = mapped_column(Text, nullable=False)
    etag: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True),
        nullable=False,
    )
