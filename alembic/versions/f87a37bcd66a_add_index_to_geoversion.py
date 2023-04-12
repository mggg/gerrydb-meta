"""Add index to GeoVersion

Revision ID: f87a37bcd66a
Revises: 7e83d298e241
Create Date: 2023-04-05 21:29:46.680630

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "f87a37bcd66a"
down_revision = "7e83d298e241"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("geo_version_geo_id", "geo_version", ["geo_id"], schema="gerrydb")


def downgrade() -> None:
    op.drop_index("geo_version_geo_id", schema="gerrydb")
