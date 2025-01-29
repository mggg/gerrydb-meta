"""SQL table definitions for GerryDB."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from geoalchemy2 import Geography as SqlGeography
from sqlalchemy import JSON, BigInteger, Boolean, CheckConstraint, DateTime, text, event
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
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session
from sqlalchemy.sql import func

from gerrydb_meta.enums import (
    ColumnKind,
    ColumnType,
    NamespaceGroup,
    ScopeType,
    ViewRenderStatus,
)
from gerrydb_meta.utils import create_column_value_partition_text

SCHEMA= "gerrydb"
metadata_obj = MetaData(schema=SCHEMA)


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
        default=uuid4,
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
    """
    Keeps track of the kind of layer for the geography in the given locality.
    So if we were to have the locality as some state, then the layer might
    be something like "tract". We can then map geometries to this layer.
    """

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

    @property
    def full_path(self):
        """Path with namespace prefix."""
        return f"/{self.namespace.path}/{self.path}"


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
    geo: Mapped["Geography"] = relationship("Geography", lazy="joined")


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
    geography = mapped_column(SqlGeography(srid=4269), nullable=True)
    internal_point = mapped_column(
        SqlGeography(geometry_type="POINT", srid=4269), nullable=True
    )

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

@event.listens_for(Geography, "after_insert")
def create_geo_partition_in_column_value(mapper, connection, geo):
    geo_id=geo.geo_id
    Session.object_session(geo).execute(create_column_value_partition_text(geo_id=geo_id))

class GeoImport(Base):
    __tablename__ = "geo_import"

    import_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[UUID] = mapped_column(
        postgresql.UUID(as_uuid=True),
        index=True,
        unique=True,
        nullable=False,
        default=uuid4,
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
    __table_args__ = (UniqueConstraint("col_id", "geo_id", "valid_from"),
                      {"postgresql_partition_by": "LIST (geo_id)" })

    col_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("column.col_id"),
        nullable=False,
        primary_key=True,
    )
    geo_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("geography.geo_id"),
        nullable=False,
        primary_key=True,
    )
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )
    valid_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        primary_key=True,
    )
    valid_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    val_float: Mapped[float] = mapped_column(postgresql.DOUBLE_PRECISION, nullable=True)
    val_int: Mapped[int] = mapped_column(BigInteger, nullable=True)
    val_str: Mapped[str] = mapped_column(Text, nullable=True)
    val_bool: Mapped[bool] = mapped_column(Boolean, nullable=True)

    meta: Mapped[ObjectMeta] = relationship("ObjectMeta")


class Plan(Base):
    __tablename__ = "plan"
    __table_args__ = (UniqueConstraint("namespace_id", "path"),)

    plan_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False, index=True
    )
    path: Mapped[int] = mapped_column(Text, nullable=False, index=True)
    set_version_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("geo_set_version.set_version_id"),
        nullable=False,
        index=True,
    )
    num_districts: Mapped[int] = mapped_column(Integer, nullable=False)
    complete: Mapped[bool] = mapped_column(Boolean, nullable=False)

    description: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str | None] = mapped_column(String(2048))  # e.g. from Districtr
    districtr_id: Mapped[str | None] = mapped_column(Text)
    daves_id: Mapped[str | None] = mapped_column(Text)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )
    # TODO: should plans be versioned?
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")
    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    set_version: Mapped[GeoSetVersion] = relationship("GeoSetVersion", lazy="joined")
    assignments: Mapped[list["PlanAssignment"]] = relationship(
        "PlanAssignment", lazy="joined"
    )


class PlanAssignment(Base):
    __tablename__ = "plan_assignment"

    plan_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("plan.plan_id"), primary_key=True
    )
    geo_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geography.geo_id"), primary_key=True
    )
    assignment: Mapped[str] = mapped_column(Text, nullable=False)

    geo: Mapped["Geography"] = relationship("Geography", lazy="joined")


class Graph(Base):
    __tablename__ = "graph"
    __table_args__ = (UniqueConstraint("namespace_id", "path"),)

    graph_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    set_version_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("geo_set_version.set_version_id"),
        nullable=False,
        index=True,
    )
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False, index=True
    )
    path: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    proj: Mapped[str | None] = mapped_column(Text, nullable=True)

    edges: Mapped[list["GraphEdge"]] = relationship("GraphEdge")
    set_version: Mapped[GeoSetVersion] = relationship("GeoSetVersion")
    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")
    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")

    @property
    def full_path(self):
        """Path with namespace prefix."""
        return f"/{self.namespace.path}/{self.path}"


class GraphEdge(Base):
    __tablename__ = "graph_edge"

    graph_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("graph.graph_id"), primary_key=True
    )
    geo_id_1: Mapped[int] = mapped_column(
        Integer, ForeignKey("geography.geo_id"), primary_key=True
    )
    geo_id_2: Mapped[int] = mapped_column(
        Integer, ForeignKey("geography.geo_id"), primary_key=True
    )
    weights: Mapped[Any | None] = mapped_column(postgresql.JSONB)

    graph: Mapped[Graph] = relationship("Graph", overlaps="edges")
    geo_1: Mapped[Geography] = relationship(
        "Geography", lazy="joined", foreign_keys="GraphEdge.geo_id_1"
    )
    geo_2: Mapped[Geography] = relationship(
        "Geography", lazy="joined", foreign_keys="GraphEdge.geo_id_2"
    )


class Ensemble(Base):
    __tablename__ = "ensemble"
    __table_args__ = (UniqueConstraint("namespace_id", "path"),)

    ensemble_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    namespace_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id"), nullable=False, index=True
    )
    path: Mapped[int] = mapped_column(Text, nullable=False, index=True)
    graph_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("graph.graph_id"), nullable=False, index=True
    )

    # Actual ensemble data is stored as a compressed blob in S3 or the like.
    blob_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    blob_url: Mapped[str] = mapped_column(String(2048), nullable=False)

    pop_col_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("column.col_id"), nullable=True
    )
    seed_plan_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("plan.plan_id"), nullable=True
    )
    num_districts: Mapped[int] = mapped_column(Integer, nullable=False)
    num_plans: Mapped[int] = mapped_column(Integer, nullable=False)
    params: Mapped[dict | None] = mapped_column(postgresql.JSONB, nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)

    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")
    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")


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
    # Technically redundant with (loc_id, layer_id) but very handy.
    set_version_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("geo_set_version.set_version_id"), nullable=False
    )
    at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    proj: Mapped[str | None] = mapped_column(Text)
    meta_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("meta.meta_id"), nullable=False
    )
    graph_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("graph.graph_id"), nullable=True
    )
    # Essentially a checksum.
    num_geos: Mapped[int] = mapped_column(Integer, nullable=False)

    namespace: Mapped[Namespace] = relationship("Namespace", lazy="joined")
    template: Mapped[ViewTemplate] = relationship("ViewTemplate", lazy="joined")
    template_version: Mapped[ViewTemplateVersion] = relationship(
        "ViewTemplateVersion", lazy="joined"
    )
    loc: Mapped[Locality] = relationship("Locality", lazy="joined")
    layer: Mapped[GeoLayer] = relationship("GeoLayer", lazy="joined")
    meta: Mapped[ObjectMeta] = relationship("ObjectMeta", lazy="joined")
    graph: Mapped[Graph | None] = relationship("Graph", lazy="joined")


class ViewRender(Base):
    __tablename__ = "view_render"

    render_id: Mapped[UUID] = mapped_column(
        postgresql.UUID(as_uuid=True), primary_key=True
    )
    view_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("view.view_id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[int] = mapped_column(
        Integer, ForeignKey("user.user_id"), nullable=False
    )
    # e.g. local filesystem, S3, ...
    path: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[ViewRenderStatus] = mapped_column(
        SqlEnum(ViewRenderStatus), nullable=False
    )


"""
class ViewSet(Base):
    __tablename__ = "view_set"

    render_id: Mapped[UUID] = mapped_column(
        postgresql.UUID(as_uuid=True), primary_key=True
    )
    view_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("view.view_id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[int] = mapped_column(
        Integer, ForeignKey("user.user_id"), nullable=False
    )
    # e.g. local filesystem, S3, ...
    path: Mapped[str] = mapped_column(Text, nullable=False)
    # job_status: Mapped[]
"""


class ETag(Base):
    __tablename__ = "etag"
    __table_args__ = (UniqueConstraint("namespace_id", "table"),)

    etag_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    namespace_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("namespace.namespace_id")
    )
    table: Mapped[str] = mapped_column(Text, nullable=False)
    etag: Mapped[UUID] = mapped_column(
        postgresql.UUID(as_uuid=True),
        nullable=False,
    )
