import pytest

from tests.functional.adapter.utils.base_utils import BaseUtils
from tests.functional.adapter.utils.fixture_any_value import (
    models__test_any_value_sql,
    models__test_any_value_yml,
    seeds__data_any_value_csv,
    seeds__data_any_value_expected_csv,
)


class BaseAnyValue(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_any_value.csv": seeds__data_any_value_csv,
            "data_any_value_expected.csv": seeds__data_any_value_expected_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_any_value.yml": models__test_any_value_yml,
            "test_any_value.sql": self.interpolate_macro_namespace(
                models__test_any_value_sql, "any_value"
            ),
        }


class TestAnyValue(BaseAnyValue):
    pass
