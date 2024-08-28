import pytest

from tests.functional.adapter.utils.base_utils import BaseUtils
from tests.functional.adapter.utils.fixture_right import (
    models__test_right_sql,
    models__test_right_yml,
    seeds__data_right_csv,
)


class BaseRight(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_right.csv": seeds__data_right_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_right.yml": models__test_right_yml,
            "test_right.sql": self.interpolate_macro_namespace(models__test_right_sql, "right"),
        }


class TestRight(BaseRight):
    pass
