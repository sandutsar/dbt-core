import pytest

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import CompilationError
from dbt.tests.util import get_manifest, rm_file, run_dbt, write_file
from tests.functional.partial_parsing.fixtures import (
    metric_model_a_sql,
    metricflow_time_spine_sql,
    people_metrics2_yml,
    people_metrics3_yml,
    people_metrics_yml,
    people_semantic_models_yml,
    people_sl_yml,
    people_sql,
)


class TestMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_metrics(self, project):
        # initial run
        results = run_dbt(["run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 2

        # Add metrics yaml file (and necessary semantic models yaml)
        write_file(
            people_semantic_models_yml,
            project.project_root,
            "models",
            "people_semantic_models.yml",
        )
        write_file(people_metrics_yml, project.project_root, "models", "people_metrics.yml")
        results = run_dbt(["run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        assert len(manifest.metrics) == 2
        metric_people_id = "metric.test.number_of_people"
        metric_people = manifest.metrics[metric_people_id]
        expected_meta = {"my_meta": "testing"}
        assert metric_people.meta == expected_meta

        # TODO: Bring back when we resolving `depends_on_nodes`
        # metric_tenure_id = "metric.test.collective_tenure"
        # metric_tenure = manifest.metrics[metric_tenure_id]
        # assert metric_people.refs == [RefArgs(name="people")]
        # assert metric_tenure.refs == [RefArgs(name="people")]
        # expected_depends_on_nodes = ["model.test.people"]
        # assert metric_people.depends_on.nodes == expected_depends_on_nodes

        # Change metrics yaml files
        write_file(people_metrics2_yml, project.project_root, "models", "people_metrics.yml")
        results = run_dbt(["run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        metric_people = manifest.metrics[metric_people_id]
        expected_meta = {"my_meta": "replaced"}
        assert metric_people.meta == expected_meta
        # TODO: Bring back when we resolving `depends_on_nodes`
        # expected_depends_on_nodes = ["model.test.people"]
        # assert metric_people.depends_on.nodes == expected_depends_on_nodes

        # Add model referring to metric
        write_file(metric_model_a_sql, project.project_root, "models", "metric_model_a.sql")
        results = run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        # TODO: Bring back when we resolving `depends_on_nodes`
        # model_a = manifest.nodes["model.test.metric_model_a"]
        # expected_depends_on_nodes = [
        #     "metric.test.number_of_people",
        #     "metric.test.collective_tenure",
        # ]
        # assert model_a.depends_on.nodes == expected_depends_on_nodes

        # Then delete a metric
        write_file(people_metrics3_yml, project.project_root, "models", "people_metrics.yml")
        with pytest.raises(CompilationError):
            # We use "parse" here and not "run" because we're checking that the CompilationError
            # occurs at parse time, not compilation
            results = run_dbt(["parse"])


class TestDeleteFileWithMetricsAndSemanticModels:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "people_sl.yml": people_sl_yml,
        }

    def test_metrics(self, project):
        # Initial parsing
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert result.success
        manifest = result.result
        assert isinstance(manifest, Manifest)
        assert len(manifest.metrics) == 3

        # Remove metric file
        rm_file(project.project_root, "models", "people_sl.yml")

        # Rerun parse, shouldn't fail
        result = runner.invoke(["parse"])
        assert result.exception is None, result.exception
