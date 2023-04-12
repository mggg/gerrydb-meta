"""Add set_version_id to View

Revision ID: 96d3750366ff
Revises: f87a37bcd66a
Create Date: 2023-04-10 15:06:09.833837

"""
from sqlalchemy import Column, ForeignKey, Integer

from alembic import op

# revision identifiers, used by Alembic.
revision = "96d3750366ff"
down_revision = "f87a37bcd66a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "view",
        Column(
            "set_version_id",
            Integer(),
            ForeignKey("gerrydb.geo_set_version.set_version_id"),
        ),
        schema="gerrydb",
    )


def downgrade() -> None:
    op.drop_column("view", "set_version_id", schema="gerrydb")
