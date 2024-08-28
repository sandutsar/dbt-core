import os
import shutil
from copy import deepcopy
from typing import List

import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt.tests.util import run_dbt, write_file
from dbt_common.events.base_types import BaseEvent
from dbt_semantic_interfaces.type_enums.export_destination_type import (
    ExportDestinationType,
)
from tests.functional.assertions.test_runner import dbtTestRunner
from tests.functional.saved_queries.fixtures import (
    saved_queries_with_defaults_yml,
    saved_queries_with_diff_filters_yml,
    saved_queries_yml,
    saved_query_description,
    saved_query_with_cache_configs_defined_yml,
)
from tests.functional.semantic_models.fixtures import (
    fct_revenue_sql,
    metricflow_time_spine_sql,
    schema_yml,
)


class TestSavedQueryParsing:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "saved_queries.yml": saved_queries_yml,
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "docs.md": saved_query_description,
        }

    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "prod": deepcopy(dbt_profile_target)}
        outputs["default"]["schema"] = unique_schema
        outputs["prod"]["schema"] = other_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    def copy_state(self):
        if not os.path.exists("state"):
            os.makedirs("state")
        shutil.copyfile("target/manifest.json", "state/manifest.json")

    def test_semantic_model_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse", "--no-partial-parse"])
        assert result.success
        assert isinstance(result.result, Manifest)
        manifest = result.result
        assert len(manifest.saved_queries) == 1
        saved_query = manifest.saved_queries["saved_query.test.test_saved_query"]
        assert saved_query.name == "test_saved_query"
        assert len(saved_query.query_params.metrics) == 1
        assert len(saved_query.query_params.group_by) == 1
        assert len(saved_query.query_params.where.where_filters) == 3
        assert len(saved_query.depends_on.nodes) == 1
        assert saved_query.description == "My SavedQuery Description"
        assert len(saved_query.exports) == 1
        assert saved_query.exports[0].name == "my_export"
        assert saved_query.exports[0].config.alias == "my_export_alias"
        assert saved_query.exports[0].config.export_as == ExportDestinationType.TABLE
        assert saved_query.exports[0].config.schema_name == "my_export_schema_name"
        assert saved_query.exports[0].unrendered_config == {
            "alias": "my_export_alias",
            "export_as": "table",
            "schema": "my_export_schema_name",
        }

        # Save state
        self.copy_state()
        # Nothing has changed, so no state:modified results
        results = run_dbt(["ls", "--select", "state:modified", "--state", "./state"])
        assert len(results) == 0

        # Change saved_query
        write_file(
            saved_query_with_cache_configs_defined_yml,
            project.project_root,
            "models",
            "saved_queries.yml",
        )
        # State modified finds changed saved_query
        results = run_dbt(["ls", "--select", "state:modified", "--state", "./state"])
        assert len(results) == 1

        # change exports
        write_file(
            saved_queries_with_defaults_yml, project.project_root, "models", "saved_queries.yml"
        )
        # State modified finds changed saved_query
        results = run_dbt(["ls", "--select", "state:modified", "--state", "./state"])
        assert len(results) == 1

    def test_semantic_model_parsing_change_export(self, project, other_schema):
        runner = dbtTestRunner()
        result = runner.invoke(["parse", "--no-partial-parse"])
        assert result.success
        assert isinstance(result.result, Manifest)
        manifest = result.result
        assert len(manifest.saved_queries) == 1
        saved_query = manifest.saved_queries["saved_query.test.test_saved_query"]
        assert saved_query.name == "test_saved_query"
        assert saved_query.exports[0].name == "my_export"

        # Save state
        self.copy_state()
        # Nothing has changed, so no state:modified results
        results = run_dbt(["ls", "--select", "state:modified", "--state", "./state"])
        assert len(results) == 0

        # Change export name
        write_file(
            saved_queries_yml.replace("name: my_export", "name: my_expor2"),
            project.project_root,
            "models",
            "saved_queries.yml",
        )
        # State modified finds changed saved_query
        results = run_dbt(["ls", "--select", "state:modified", "--state", "./state"])
        assert len(results) == 1

        # Change export schema
        write_file(
            saved_queries_yml.replace(
                "schema: my_export_schema_name", "schema: my_export_schema_name2"
            ),
            project.project_root,
            "models",
            "saved_queries.yml",
        )
        # State modified finds changed saved_query
        results = run_dbt(["ls", "--select", "state:modified", "--state", "./state"])
        assert len(results) == 1

    def test_semantic_model_parsing_with_default_schema(self, project, other_schema):
        write_file(
            saved_queries_with_defaults_yml, project.project_root, "models", "saved_queries.yml"
        )
        runner = dbtTestRunner()
        result = runner.invoke(["parse", "--no-partial-parse", "--target", "prod"])
        assert result.success
        assert isinstance(result.result, Manifest)
        manifest = result.result
        assert len(manifest.saved_queries) == 1
        saved_query = manifest.saved_queries["saved_query.test.test_saved_query"]
        assert saved_query.name == "test_saved_query"
        assert len(saved_query.query_params.metrics) == 1
        assert len(saved_query.query_params.group_by) == 1
        assert len(saved_query.query_params.where.where_filters) == 3
        assert len(saved_query.depends_on.nodes) == 1
        assert saved_query.description == "My SavedQuery Description"
        assert len(saved_query.exports) == 1
        assert saved_query.exports[0].name == "my_export"
        assert saved_query.exports[0].config.alias == "my_export_alias"
        assert saved_query.exports[0].config.export_as == ExportDestinationType.TABLE
        assert saved_query.exports[0].config.schema_name == other_schema
        assert saved_query.exports[0].unrendered_config == {
            "alias": "my_export_alias",
            "export_as": "table",
        }

        # Save state
        self.copy_state()
        # Nothing has changed, so no state:modified results
        results = run_dbt(
            ["ls", "--select", "state:modified", "--state", "./state", "--target", "prod"]
        )
        assert len(results) == 0

        # There should also be no state:modified results when using the default schema
        results = run_dbt(["ls", "--select", "state:modified", "--state", "./state"])
        assert len(results) == 0

    def test_saved_query_error(self, project):
        error_schema_yml = saved_queries_yml.replace("simple_metric", "metric_not_found")
        write_file(error_schema_yml, project.project_root, "models", "saved_queries.yml")
        events: List[BaseEvent] = []
        runner = dbtTestRunner(callbacks=[events.append])

        result = runner.invoke(["parse", "--no-partial-parse"])
        assert not result.success
        validation_errors = [e for e in events if e.info.name == "MainEncounteredError"]
        assert validation_errors


class TestSavedQueryPartialParsing:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "saved_queries.yml": saved_queries_yml,
            "saved_queries_with_diff_filters.yml": saved_queries_with_diff_filters_yml,
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "docs.md": saved_query_description,
        }

    def test_saved_query_filter_types(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success

        manifest = result.result
        saved_query1 = manifest.saved_queries["saved_query.test.test_saved_query_where_list"]
        saved_query2 = manifest.saved_queries["saved_query.test.test_saved_query_where_str"]

        # List filter
        assert len(saved_query1.query_params.where.where_filters) == 2
        assert {
            where_filter.where_sql_template
            for where_filter in saved_query1.query_params.where.where_filters
        } == {
            "{{ TimeDimension('id__ds', 'DAY') }} <= now()",
            "{{ TimeDimension('id__ds', 'DAY') }} >= '2023-01-01'",
        }
        # String filter
        assert len(saved_query2.query_params.where.where_filters) == 1
        assert (
            saved_query2.query_params.where.where_filters[0].where_sql_template
            == "{{ TimeDimension('id__ds', 'DAY') }} <= now()"
        )

    def test_saved_query_metrics_changed(self, project):
        # First, use the default saved_queries.yml to define our saved_queries, and
        # run the dbt parse command
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success

        # Next, modify the default saved_queries.yml to change a detail of the saved
        # query.
        modified_saved_queries_yml = saved_queries_yml.replace("simple_metric", "txn_revenue")
        write_file(modified_saved_queries_yml, project.project_root, "models", "saved_queries.yml")

        # Now, run the dbt parse command again.
        result = runner.invoke(["parse"])
        assert result.success

        # Finally, verify that the manifest reflects the partially parsed change
        manifest = result.result
        saved_query = manifest.saved_queries["saved_query.test.test_saved_query"]
        assert len(saved_query.metrics) == 1
        assert saved_query.metrics[0] == "txn_revenue"

    def test_saved_query_deleted_partial_parsing(self, project):
        # First, use the default saved_queries.yml to define our saved_query, and
        # run the dbt parse command
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert "saved_query.test.test_saved_query" in result.result.saved_queries

        # Next, modify the default saved_queries.yml to remove the saved query.
        write_file("", project.project_root, "models", "saved_queries.yml")

        # Now, run the dbt parse command again.
        result = runner.invoke(["parse"])
        assert result.success

        # Finally, verify that the manifest reflects the deletion
        assert "saved_query.test.test_saved_query" not in result.result.saved_queries
