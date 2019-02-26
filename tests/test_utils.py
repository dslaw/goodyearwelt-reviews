import pytest

from src.utils import create_views


class TestCreateViews(object):
    @pytest.mark.parametrize("view_name", ["media_rollups", "rollups"])
    def test_view_is_created(self, cursor, view_name):
        create_views(cursor)

        # If the query succeeds, then the view exists - the result
        # does not matter.
        cursor.execute(f"select count(*) from {view_name}")
        cursor.fetchone()
