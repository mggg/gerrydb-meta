"""Create localities

Revision ID: d3b6ebaf041f
Revises: 7367a058533d
Create Date: 2023-03-22 14:36:48.946268

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "d3b6ebaf041f"
down_revision = "7367a058533d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "locality",
        sa.Column("loc_id", sa.Integer(), nullable=False),
        sa.Column("canonical_ref_id", sa.Integer(), nullable=False),
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("default_proj", sa.Text(), nullable=True),
        sa.CheckConstraint("parent_id <> loc_id"),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["gerrydb.meta.meta_id"],
        ),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["gerrydb.locality.loc_id"],
        ),
        sa.PrimaryKeyConstraint("loc_id"),
        schema="gerrydb",
    )
    op.create_index(
        op.f("ix_gerrydb_locality_canonical_ref_id"),
        "locality",
        ["canonical_ref_id"],
        unique=True,
        schema="gerrydb",
    )
    op.create_table(
        "locality_ref",
        sa.Column("ref_id", sa.Integer(), nullable=False),
        sa.Column("loc_id", sa.Integer(), nullable=True),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("meta_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["loc_id"],
            ["gerrydb.locality.loc_id"],
        ),
        sa.ForeignKeyConstraint(
            ["meta_id"],
            ["gerrydb.meta.meta_id"],
        ),
        sa.PrimaryKeyConstraint("ref_id"),
        schema="gerrydb",
    )
    op.create_index(
        op.f("ix_gerrydb_locality_ref_path"),
        "locality_ref",
        ["path"],
        unique=True,
        schema="gerrydb",
    )
    op.create_foreign_key(
        op.f("fk_locality_locality_ref__canonical_ref_id"),
        "locality",
        "locality_ref",
        ["canonical_ref_id"],
        ["ref_id"],
        source_schema="gerrydb",
        referent_schema="gerrydb",
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_gerrydb_locality_ref_path"),
        table_name="locality_ref",
        schema="gerrydb",
    )
    op.drop_table("locality_ref", schema="gerrydb")
    op.drop_index(
        op.f("ix_gerrydb_locality_canonical_ref_id"),
        table_name="locality",
        schema="gerrydb",
    )
    op.drop_table("locality", schema="gerrydb")
