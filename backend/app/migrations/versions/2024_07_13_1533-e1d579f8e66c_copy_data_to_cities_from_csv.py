"""Copy data to cities from .csv

Revision ID: e1d579f8e66c
Revises: 2fd6d9ab6e39
Create Date: 2024-07-13 15:33:31.663710

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e1d579f8e66c'
down_revision: Union[str, None] = '2fd6d9ab6e39'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
