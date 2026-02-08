"""add category to post_summaries

Revision ID: 20260208_183500
Revises: 20260203_162304
Create Date: 2026-02-08 18:35:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260208_183500"
down_revision = "20260203_162304"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "post_summaries",
        sa.Column("category", sa.Text(), server_default=sa.text("'OTHER_USEFUL'"), nullable=True),
    )
    op.execute(
        """
        UPDATE post_summaries
        SET category = 'OTHER_USEFUL'
        WHERE category IS NULL OR btrim(category) = ''
        """
    )
    op.alter_column("post_summaries", "category", nullable=False)


def downgrade() -> None:
    op.drop_column("post_summaries", "category")
