"""Create tabular schema

Revision ID: 8dd630f55d05
Revises: f92609b1a9bd
Create Date: 2023-03-22 14:47:10.447269

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "8dd630f55d05"
down_revision = "f92609b1a9bd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "column",
        sa.Column("col_id", sa.Integer(), nullable=False),
        sa.Column("namespace_id", sa.Integer(), nullable=False),
        sa.Column("canonical_ref_id", sa.Integer(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("source_url", sa.String(length=2048), nullable=True),
        sa.Column(
            "kind",
            sa.Enum(
                "COUNT",
                "PERCENT",
                "CATEGORICAL",
                "IDENTIFIER",
                "AREA",
                "OTHER",
                name="columnkind",
            ),
            nullable=False,
        ),
        sa.Column(
            "type",
            sa.Enum("FLOAT", "INT", "BOOL", "STR", "JSON", name="columntype"),
            nullable=False,
        ),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.ForeignKeyConstraint(
            ["namespace_id"],
            ["cherrydb.namespace.namespace_id"],
        ),
        sa.PrimaryKeyConstraint("col_id"),
        schema="cherrydb",
    )
    op.create_index(
        op.f("ix_cherrydb_column_canonical_ref_id"),
        "column",
        ["canonical_ref_id"],
        unique=True,
        schema="cherrydb",
    )
    op.create_table(
        "column_ref",
        sa.Column("ref_id", sa.Integer(), nullable=False),
        sa.Column("namespace_id", sa.Integer(), nullable=False),
        sa.Column("col_id", sa.Integer(), nullable=True),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["col_id"],
            ["cherrydb.column.col_id"],
        ),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.ForeignKeyConstraint(
            ["namespace_id"],
            ["cherrydb.namespace.namespace_id"],
        ),
        sa.PrimaryKeyConstraint("ref_id"),
        sa.UniqueConstraint("namespace_id", "path"),
        schema="cherrydb",
    )
    op.create_foreign_key(
        op.f("fk_column_column_ref__canonical_ref_id"),
        "column_ref",
        "column",
        ["ref_id"],
        ["canonical_ref_id"],
        source_schema="cherrydb",
        referent_schema="cherrydb",
    )
    op.create_index(
        op.f("ix_cherrydb_column_ref_path"),
        "column_ref",
        ["path"],
        unique=False,
        schema="cherrydb",
    )
    op.create_table(
        "column_relation",
        sa.Column("relation_id", sa.Integer(), nullable=False),
        sa.Column("namespace_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("expr", sa.JSON(), nullable=False),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.ForeignKeyConstraint(
            ["namespace_id"],
            ["cherrydb.namespace.namespace_id"],
        ),
        sa.PrimaryKeyConstraint("relation_id"),
        sa.UniqueConstraint("namespace_id", "name"),
        schema="cherrydb",
    )
    op.create_table(
        "column_relation_member",
        sa.Column("relation_id", sa.Integer(), nullable=False),
        sa.Column("member_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["member_id"],
            ["cherrydb.column.col_id"],
        ),
        sa.ForeignKeyConstraint(
            ["relation_id"],
            ["cherrydb.column_relation.relation_id"],
        ),
        sa.PrimaryKeyConstraint("relation_id", "member_id"),
        schema="cherrydb",
    )
    op.create_table(
        "column_set",
        sa.Column("set_id", sa.Integer(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("namespace_id", sa.Integer(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.ForeignKeyConstraint(
            ["namespace_id"],
            ["cherrydb.namespace.namespace_id"],
        ),
        sa.PrimaryKeyConstraint("set_id"),
        sa.UniqueConstraint("path", "namespace_id"),
        schema="cherrydb",
    )
    op.create_table(
        "column_set_member",
        sa.Column("set_id", sa.Integer(), nullable=False),
        sa.Column("ref_id", sa.Integer(), nullable=False),
        sa.Column("order", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["ref_id"],
            ["cherrydb.column_ref.ref_id"],
        ),
        sa.ForeignKeyConstraint(
            ["set_id"],
            ["cherrydb.column_set.set_id"],
        ),
        sa.PrimaryKeyConstraint("set_id", "ref_id"),
        schema="cherrydb",
    )
    op.create_table(
        "column_value",
        sa.Column("val_id", sa.Integer(), nullable=False),
        sa.Column("col_id", sa.Integer(), nullable=False),
        sa.Column("geo_id", sa.Integer(), nullable=False),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("val_float", sa.DOUBLE_PRECISION(), nullable=True),
        sa.Column("val_int", sa.BigInteger(), nullable=True),
        sa.Column("val_str", sa.Text(), nullable=True),
        sa.Column("val_bool", sa.Boolean(), nullable=True),
        sa.Column("val_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(
            ["col_id"],
            ["cherrydb.column.col_id"],
        ),
        sa.ForeignKeyConstraint(
            ["geo_id"],
            ["cherrydb.geography.geo_id"],
        ),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["cherrydb.meta.meta_id"],
        ),
        sa.PrimaryKeyConstraint("val_id"),
        sa.UniqueConstraint("col_id", "geo_id", "valid_from"),
        schema="cherrydb",
    )


def downgrade() -> None:
    op.drop_table("column_value", schema="cherrydb")
    op.drop_table("column_set_member", schema="cherrydb")
    op.drop_table("column_set", schema="cherrydb")
    op.drop_table("column_relation_member", schema="cherrydb")
    op.drop_table("column_relation", schema="cherrydb")
    op.drop_index(
        op.f("ix_cherrydb_column_ref_path"), table_name="column_ref", schema="cherrydb"
    )
    op.drop_table("column_ref", schema="cherrydb")
    op.drop_index(
        op.f("ix_cherrydb_column_canonical_ref_id"),
        table_name="column",
        schema="cherrydb",
    )
    op.drop_table("column", schema="cherrydb")
    op.execute("DROP TYPE columnkind")
    op.execute("DROP TYPE columntype")
