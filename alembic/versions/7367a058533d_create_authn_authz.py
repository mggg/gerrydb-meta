"""Create authentication/authorization models

Revision ID: 7367a058533d
Revises: 
Create Date: 2023-03-21 17:25:22.277920

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "7367a058533d"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create user model.
    op.create_table(
        "user",
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("email", sa.String(length=254), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("user_id"),
        schema="cherrydb",
    )
    op.create_index(
        op.f("ix_cherrydb_user_email"),
        "user",
        ["email"],
        unique=True,
        schema="cherrydb",
    )

    # Create metadata model (dependency for group models, etc.)
    op.create_table(
        "meta",
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.Column("uuid", sa.UUID(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["created_by"],
            ["cherrydb.user.user_id"],
        ),
        sa.PrimaryKeyConstraint("meta_id"),
        schema="cherrydb",
    )
    op.create_index(
        op.f("ix_cherrydb_meta_uuid"), "meta", ["uuid"], unique=True, schema="cherrydb"
    )

    # Create user group models.
    op.create_table(
        "user_group",
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.PrimaryKeyConstraint("group_id"),
        schema="cherrydb",
    )
    op.create_table(
        "user_group_member",
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["group_id"],
            ["cherrydb.user_group.group_id"],
        ),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["cherrydb.user.user_id"],
        ),
        sa.PrimaryKeyConstraint("user_id", "group_id"),
        schema="cherrydb",
    )

    # Create namespace model (dependency for scope models).
    op.create_table(
        "namespace",
        sa.Column("namespace_id", sa.Integer(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("public", sa.Boolean(), nullable=False),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.PrimaryKeyConstraint("namespace_id"),
        schema="cherrydb",
    )
    op.create_index(
        op.f("ix_cherrydb_namespace_path"),
        "namespace",
        ["path"],
        unique=True,
        schema="cherrydb",
    )

    # Create scope models.
    op.create_table(
        "user_scope",
        sa.Column("user_perm_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column(
            "scope",
            sa.Enum(
                "NAMESPACE_READ",
                "NAMESPACE_WRITE",
                "NAMESPACE_WRITE_DERIVED",
                "NAMESPACE_CREATE",
                "LOCALITY_READ",
                "LOCALITY_WRITE",
                "META_READ",
                "META_WRITE",
                "ALL",
                name="scopetype",
            ),
            nullable=False,
        ),
        sa.Column(
            "namespace_group",
            sa.Enum("PUBLIC", "PRIVATE", "ALL", name="namespacegroup"),
            nullable=True,
        ),
        sa.Column("namespace_id", sa.Integer(), nullable=True),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.ForeignKeyConstraint(
            ["namespace_id"],
            ["cherrydb.namespace.namespace_id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["cherrydb.user.user_id"],
        ),
        sa.PrimaryKeyConstraint("user_perm_id"),
        sa.UniqueConstraint("user_id", "scope", "namespace_id"),
        schema="cherrydb",
    )
    op.create_table(
        "user_group_scope",
        sa.Column("group_perm_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.Column(
            "scope",
            sa.Enum(
                "NAMESPACE_READ",
                "NAMESPACE_WRITE",
                "NAMESPACE_WRITE_DERIVED",
                "NAMESPACE_CREATE",
                "LOCALITY_READ",
                "LOCALITY_WRITE",
                "META_READ",
                "META_WRITE",
                "ALL",
                name="scopetype",
            ),
            nullable=False,
        ),
        sa.Column(
            "namespace_group",
            sa.Enum("PUBLIC", "PRIVATE", "ALL", name="namespacegroup"),
            nullable=True,
        ),
        sa.Column("namespace_id", sa.Integer(), nullable=True),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["group_id"],
            ["cherrydb.user_group.group_id"],
        ),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.ForeignKeyConstraint(
            ["namespace_id"],
            ["cherrydb.namespace.namespace_id"],
        ),
        sa.PrimaryKeyConstraint("group_perm_id"),
        sa.UniqueConstraint("group_id", "scope", "namespace_id"),
        schema="cherrydb",
    )

    # Create API key model.
    op.create_table(
        "api_key",
        sa.Column("key_hash", sa.LargeBinary(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["cherrydb.user.user_id"],
        ),
        sa.PrimaryKeyConstraint("key_hash"),
        schema="cherrydb",
    )


def downgrade() -> None:
    op.drop_table("api_key", schema="cherrydb")
    op.drop_table("user_group_scope", schema="cherrydb")
    op.drop_table("user_scope", schema="cherrydb")
    op.drop_index(
        op.f("ix_cherrydb_namespace_path"), table_name="namespace", schema="cherrydb"
    )
    op.drop_table("namespace", schema="cherrydb")
    op.drop_table("user_group_member", schema="cherrydb")
    op.drop_table("user_group", schema="cherrydb")
    op.drop_index(op.f("ix_cherrydb_meta_uuid"), table_name="meta", schema="cherrydb")
    op.drop_table("meta", schema="cherrydb")
    op.drop_index(op.f("ix_cherrydb_user_email"), table_name="user", schema="cherrydb")
    op.drop_table("user", schema="cherrydb")
    op.execute("DROP TYPE scopetype")
    op.execute("DROP TYPE namespacegroup")
