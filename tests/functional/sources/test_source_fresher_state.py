import os
import json
import pytest
from datetime import datetime, timedelta

import dbt.version
from tests.functional.sources.common_source_setup import BaseSourcesTest

# from tests.functional.sources.fixtures import (
#     error_models__schema_yml,
#     error_models__model_sql,
#     filtered_models__schema_yml,
#     override_freshness_models__schema_yml,
# )

# TODO: We may create utility classes to handle reusable fixtures.


# put these here for now to get tests working
class AnyStringWith:
    def __init__(self, contains=None):
        self.contains = contains

    def __eq__(self, other):
        if not isinstance(other, str):
            return False

        if self.contains is None:
            return True

        return self.contains in other

    def __repr__(self):
        return "AnyStringWith<{!r}>".format(self.contains)


class AnyFloat:
    """Any float. Use this in assertEqual() calls to assert that it is a float."""

    def __eq__(self, other):
        return isinstance(other, float)


class SuccessfulSourceFreshnessTest(BaseSourcesTest):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        self.run_dbt_with_vars(project, ["seed"])
        pytest._id = 101
        pytest.freshness_start_time = datetime.utcnow()
        # this is the db initial value
        pytest.last_inserted_time = "2016-09-19T14:45:51+00:00"

        os.environ["DBT_ENV_CUSTOM_ENV_key"] = "value"

        yield

        del os.environ["DBT_ENV_CUSTOM_ENV_key"]

    def _set_updated_at_to(self, project, delta):
        insert_time = datetime.utcnow() + delta
        timestr = insert_time.strftime("%Y-%m-%d %H:%M:%S")
        # favorite_color,id,first_name,email,ip_address,updated_at
        insert_id = pytest._id
        pytest._id += 1
        quoted_columns = ",".join(
            project.adapter.quote(c)
            for c in ("favorite_color", "id", "first_name", "email", "ip_address", "updated_at")
        )
        kwargs = {
            "schema": project.test_schema,
            "time": timestr,
            "id": insert_id,
            "source": project.adapter.quote("source"),
            "quoted_columns": quoted_columns,
        }
        raw_sql = """INSERT INTO {schema}.{source}
            ({quoted_columns})
        VALUES (
            'blue',{id},'Jake','abc@example.com','192.168.1.1','{time}'
        )""".format(
            **kwargs
        )
        project.run_sql(raw_sql)
        pytest.last_inserted_time = insert_time.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def assertBetween(self, timestr, start, end=None):
        datefmt = "%Y-%m-%dT%H:%M:%S.%fZ"
        if end is None:
            end = datetime.utcnow()

        parsed = datetime.strptime(timestr, datefmt)

        assert start <= parsed
        assert end >= parsed

    def _assert_freshness_results(self, path, state):
        assert os.path.exists(path)
        with open(path) as fp:
            data = json.load(fp)

        assert set(data) == {"metadata", "results", "elapsed_time"}
        assert "generated_at" in data["metadata"]
        assert isinstance(data["elapsed_time"], float)
        self.assertBetween(data["metadata"]["generated_at"], pytest.freshness_start_time)
        assert (
            data["metadata"]["dbt_schema_version"]
            == "https://schemas.getdbt.com/dbt/sources/v3.json"
        )
        assert data["metadata"]["dbt_version"] == dbt.version.__version__
        assert data["metadata"]["invocation_id"] == dbt.tracking.active_user.invocation_id
        key = "key"
        if os.name == "nt":
            key = key.upper()
        assert data["metadata"]["env"] == {key: "value"}

        last_inserted_time = pytest.last_inserted_time

        assert len(data["results"]) == 1

        # TODO: replace below calls - could they be more sane?
        # TODO: could use this as a schema template to artifically create previous and current state sources.json
        assert data["results"] == [
            {
                "unique_id": "source.test.test_source.test_table",
                "max_loaded_at": last_inserted_time,
                "snapshotted_at": AnyStringWith(),
                "max_loaded_at_time_ago_in_s": AnyFloat(),
                "status": state,
                "criteria": {
                    "filter": None,
                    "warn_after": {"count": 10, "period": "hour"},
                    "error_after": {"count": 18, "period": "hour"},
                },
                "adapter_response": {},
                "thread_id": AnyStringWith("Thread-"),
                "execution_time": AnyFloat(),
                "timing": [
                    {
                        "name": "compile",
                        "started_at": AnyStringWith(),
                        "completed_at": AnyStringWith(),
                    },
                    {
                        "name": "execute",
                        "started_at": AnyStringWith(),
                        "completed_at": AnyStringWith(),
                    },
                ],
            }
        ]


# TODO: warn_source, pass_source, error_source serve as starting points for previous state sources.json
# TODO: inherit SuccessfulSourceFreshnessTest as a base class for downstream tests
class TestSourceFreshness(SuccessfulSourceFreshnessTest):
    def test_source_freshness(self, project):
        # test_source.test_table should have a loaded_at field of `updated_at`
        # and a freshness of warn_after: 10 hours, error_after: 18 hours
        # by default, our data set is way out of date!

        results = self.run_dbt_with_vars(
            project, ["source", "freshness", "-o", "target/error_source.json"], expect_pass=False
        )
        assert len(results) == 1
        assert results[0].status == "error"
        self._assert_freshness_results("target/error_source.json", "error")

        self._set_updated_at_to(project, timedelta(hours=-12))
        results = self.run_dbt_with_vars(
            project,
            ["source", "freshness", "-o", "target/warn_source.json"],
        )
        assert len(results) == 1
        assert results[0].status == "warn"
        self._assert_freshness_results("target/warn_source.json", "warn")

        self._set_updated_at_to(project, timedelta(hours=-2))
        results = self.run_dbt_with_vars(
            project,
            ["source", "freshness", "-o", "target/pass_source.json"],
        )
        assert len(results) == 1
        assert results[0].status == "pass"
        self._assert_freshness_results("target/pass_source.json", "pass")


# TODO: Sung's tests
# Assert nothing to do if current state sources is not fresher than previous state
# - run source freshness regardless of pass,warn,error →  run `dbt build —select source_status:fresher+` → assert nothing runs

# TODO: probably add a class and inherit TestSourceFreshness or SuccessfulSourceFreshnessTest
# TODO: manipulate the sources.json path AND results to be artificially older than current state
# TODO: run_dbt_with_vars will be my go to function for running ["source_status:fresher+", "-o", "target/pass_source.json"]
# Assert all the test cases below
# - run source freshness with `pass` → run `dbt run —select source_status:fresher+` → assert downstream nodes pass and work correctly
# - run source freshness with `pass` → run `dbt run —select source_status:fresher` → assert nothing is run
# - run source freshness with `warn` → run `dbt run —select source_status:fresher+` → assert downstream nodes pass and work correctly
# - run source freshness with `warn` → run `dbt run —select source_status:fresher` → assert nothing is run
# - run source freshness with `error` → run `dbt run —select source_status:fresher+` → assert downstream nodes pass and work correctly
# - run source freshness with `error` → run `dbt run —select source_status:fresher` → assert nothing is run


# TODO: Matt's tests
# Make sure this works in combination with `state:modified+`
# - run source freshness regardless of pass,warn,error → change a model → run `dbt build —select source_status:fresher+ state:modified+` → assert downstream nodes pass and work correctly

# TODO: probably add a class and inherit TestSourceFreshness or SuccessfulSourceFreshnessTest
# TODO: manipulate the sources.json path AND results to be artificially older than current state
# TODO: run_dbt_with_vars will be my go to function for running ["source_status:fresher+", "-o", "target/pass_source.json"]
# Assert all the test cases below
# - run source freshness with `pass` → run `dbt test —select source_status:fresher+` → assert downstream nodes pass and work correctly
# - run source freshness with `pass` → run `dbt test —select source_status:fresher` → assert source tests run only
# - run source freshness with `warn` → run `dbt test —select source_status:fresher+` → assert downstream nodes pass and work correctly
# - run source freshness with `warn` → run `dbt test —select source_status:fresher` → assert source tests run only
# - run source freshness with `error` → run `dbt test —select source_status:fresher+` → assert downstream nodes pass and work correctly
# - run source freshness with `error` → run `dbt test —select source_status:fresher` → assert source tests run only


# TODO: Anais' tests
# TODO: probably add a class and inherit TestSourceFreshness or SuccessfulSourceFreshnessTest
# TODO: manipulate the sources.json path AND results to be artificially older than current state
# TODO: run_dbt_with_vars will be my go to function for running ["source_status:fresher+", "-o", "target/pass_source.json"]
# Assert all the test cases below
# - run source freshness with `pass` → run `dbt build —select source_status:fresher+` → assert downstream nodes pass and work correctly
# - run source freshness with `pass` → run `dbt build —select source_status:fresher` → assert source tests run only
# - run source freshness with `warn` → run `dbt build —select source_status:fresher+` → assert downstream nodes pass and work correctly
# - run source freshness with `warn` → run `dbt build —select source_status:fresher` → assert source tests run only
# - run source freshness with `error` → run `dbt build —select source_status:fresher+` → assert downstream nodes pass and work correctly
# - run source freshness with `error` → run `dbt build —select source_status:fresher` → assert source tests run only


# Make sure this works in combination with `result:error+`
# - run a job with an error model → run source freshness regardless of pass,warn,error → change a model to work correctly → run `dbt build —select source_status:fresher+ result:error+` → assert downstream nodes pass and work correctly

# Make sure this work in combination with `result:fail+`
# - run a job with a failed test → run source freshness regardless of pass,warn,error → change a model to pass test → run `dbt build —select source_status:fresher+ result:fail+` → assert downstream nodes pass and work correctly

# Assert intentional failure is coming through
# - `"No previous state comparison freshness results in sources.json”`

# Assert intentional failure is coming through
# - `"No current state comparison freshness results in sources.json”`
