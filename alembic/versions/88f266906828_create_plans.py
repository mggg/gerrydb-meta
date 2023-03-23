"""Create districting plans.

Revision ID: 88f266906828
Revises: 6898afa765ca
Create Date: 2023-03-23 16:57:10.921907

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "88f266906828"
down_revision = "6898afa765ca"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "plan",
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("namespace_id", sa.Integer(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("set_version_id", sa.Integer(), nullable=False),
        sa.Column("num_districts", sa.Integer(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("source_url", sa.String(length=2048), nullable=True),
        sa.Column("districtr_id", sa.Text(), nullable=True),
        sa.Column("daves_id", sa.Text(), nullable=True),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.ForeignKeyConstraint(
            ["namespace_id"],
            ["cherrydb.namespace.namespace_id"],
        ),
        sa.ForeignKeyConstraint(
            ["set_version_id"],
            ["cherrydb.geo_set_version.set_version_id"],
        ),
        sa.PrimaryKeyConstraint("plan_id"),
        sa.UniqueConstraint("namespace_id", "path"),
        schema="cherrydb",
    )
    op.create_index(
        op.f("ix_cherrydb_plan_namespace_id"),
        "plan",
        ["namespace_id"],
        unique=False,
        schema="cherrydb",
    )
    op.create_index(
        op.f("ix_cherrydb_plan_path"), "plan", ["path"], unique=False, schema="cherrydb"
    )
    op.create_index(
        op.f("ix_cherrydb_plan_set_version_id"),
        "plan",
        ["set_version_id"],
        unique=False,
        schema="cherrydb",
    )
    op.create_table(
        "plan_assignment",
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("geo_id", sa.Integer(), nullable=False),
        sa.Column("assignment", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["geo_id"],
            ["cherrydb.geography.geo_id"],
        ),
        sa.PrimaryKeyConstraint("plan_id", "geo_id"),
        schema="cherrydb",
    )


def downgrade() -> None:
    op.drop_table("plan_assignment", schema="cherrydb")
    op.drop_index(
        op.f("ix_cherrydb_plan_set_version_id"), table_name="plan", schema="cherrydb"
    )
    op.drop_index(op.f("ix_cherrydb_plan_path"), table_name="plan", schema="cherrydb")
    op.drop_index(
        op.f("ix_cherrydb_plan_namespace_id"), table_name="plan", schema="cherrydb"
    )
    op.drop_table("plan", schema="cherrydb")
