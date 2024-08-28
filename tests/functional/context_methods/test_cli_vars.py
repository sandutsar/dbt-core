import pytest
import yaml

from dbt.exceptions import CompilationError, DbtRuntimeError
from dbt.tests.fixtures.project import write_project_files
from dbt.tests.util import (
    get_artifact,
    get_logging_events,
    run_dbt,
    run_dbt_and_capture,
    write_config_file,
)
from tests.fixtures.dbt_integration_project import dbt_integration_project  # noqa: F401

models_complex__schema_yml = """
version: 2
models:
- name: complex_model
  columns:
  - name: var_1
    data_tests:
    - accepted_values:
        values:
        - abc
  - name: var_2
    data_tests:
    - accepted_values:
        values:
        - def
  - name: var_3
    data_tests:
    - accepted_values:
        values:
        - jkl
"""

models_complex__complex_model_sql = """
select
    '{{ var("variable_1") }}'::varchar as var_1,
    '{{ var("variable_2")[0] }}'::varchar as var_2,
    '{{ var("variable_3")["value"] }}'::varchar as var_3
"""

models_simple__schema_yml = """
version: 2
models:
- name: simple_model
  columns:
  - name: simple
    data_tests:
    - accepted_values:
        values:
        - abc
"""

models_simple__simple_model_sql = """
select
    '{{ var("simple") }}'::varchar as simple
"""

really_simple_model_sql = """
select 'abc' as simple
"""


class TestCLIVars:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_complex__schema_yml,
            "complex_model.sql": models_complex__complex_model_sql,
        }

    def test__cli_vars_longform(self, project):
        cli_vars = {
            "variable_1": "abc",
            "variable_2": ["def", "ghi"],
            "variable_3": {"value": "jkl"},
        }
        results = run_dbt(["run", "--vars", yaml.dump(cli_vars)])
        assert len(results) == 1
        results = run_dbt(["test", "--vars", yaml.dump(cli_vars)])
        assert len(results) == 3


class TestCLIVarsSimple:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_simple__schema_yml,
            "simple_model.sql": models_simple__simple_model_sql,
        }

    def test__cli_vars_shorthand(self, project):
        results = run_dbt(["run", "--vars", "simple: abc"])
        assert len(results) == 1
        results = run_dbt(["test", "--vars", "simple: abc"])
        assert len(results) == 1

    def test__cli_vars_longer(self, project):
        results = run_dbt(["run", "--vars", "{simple: abc, unused: def}"])
        assert len(results) == 1
        results = run_dbt(["test", "--vars", "{simple: abc, unused: def}"])
        assert len(results) == 1
        run_results = get_artifact(project.project_root, "target", "run_results.json")
        assert run_results["args"]["vars"] == {"simple": "abc", "unused": "def"}


class TestCLIVarsProfile:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_simple__schema_yml,
            "simple_model.sql": really_simple_model_sql,
        }

    def test_cli_vars_in_profile(self, project, dbt_profile_data):
        profile = dbt_profile_data
        profile["test"]["outputs"]["default"]["host"] = "{{ var('db_host') }}"
        write_config_file(profile, project.profiles_dir, "profiles.yml")
        with pytest.raises(DbtRuntimeError):
            results = run_dbt(["run"])
        results = run_dbt(["run", "--vars", "db_host: localhost"])
        assert len(results) == 1


class TestCLIVarsPackages:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root, dbt_integration_project):  # noqa: F811
        write_project_files(project_root, "dbt_integration_project", dbt_integration_project)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_simple__schema_yml,
            "simple_model.sql": really_simple_model_sql,
        }

    @pytest.fixture(scope="class")
    def packages_config(self):
        return {"packages": [{"local": "dbt_integration_project"}]}

    def test_cli_vars_in_packages(self, project, packages_config):
        # Run working deps and run commands
        run_dbt(["deps"])
        results = run_dbt(["run"])
        assert len(results) == 1

        # Change packages.yml to contain a var
        packages = packages_config
        packages["packages"][0]["local"] = "{{ var('path_to_project') }}"
        write_config_file(packages, project.project_root, "packages.yml")

        # Without vars args deps fails
        with pytest.raises(DbtRuntimeError):
            run_dbt(["deps"])

        # With vars arg deps succeeds
        results = run_dbt(["deps", "--vars", "path_to_project: dbt_integration_project"])
        assert results is None


initial_selectors_yml = """
selectors:
  - name: dev_defer_snapshots
    default: "{{ target.name == 'dev' | as_bool }}"
    definition:
      method: fqn
      value: '*'
      exclude:
        - method: config.materialized
          value: snapshot
"""

var_selectors_yml = """
selectors:
  - name: dev_defer_snapshots
    default: "{{ var('snapshot_target') == 'dev' | as_bool }}"
    definition:
      method: fqn
      value: '*'
      exclude:
        - method: config.materialized
          value: snapshot
"""


class TestCLIVarsSelectors:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_simple__schema_yml,
            "simple_model.sql": really_simple_model_sql,
        }

    @pytest.fixture(scope="class")
    def selectors(self):
        return initial_selectors_yml

    def test_vars_in_selectors(self, project):
        # initially runs ok
        results = run_dbt(["run"])
        assert len(results) == 1

        # Update the selectors.yml file to have a var
        write_config_file(var_selectors_yml, project.project_root, "selectors.yml")
        with pytest.raises(CompilationError):
            run_dbt(["run"])

        # Var in cli_vars works
        results = run_dbt(["run", "--vars", "snapshot_target: dev"])
        assert len(results) == 1


models_scrubbing__schema_yml = """
version: 2
models:
- name: simple_model
  columns:
  - name: simple
    data_tests:
    - accepted_values:
        values:
        - abc
"""

models_scrubbing__simple_model_sql = """
select
    '{{ var("DBT_ENV_SECRET_simple") }}'::varchar as simple
"""


class TestCLIVarsScrubbing:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_scrubbing__schema_yml,
            "simple_model.sql": models_scrubbing__simple_model_sql,
        }

    def test__run_results_scrubbing(self, project):
        results, output = run_dbt_and_capture(
            [
                "--debug",
                "--log-format",
                "json",
                "run",
                "--vars",
                "{DBT_ENV_SECRET_simple: abc, unused: def}",
            ]
        )
        assert len(results) == 1

        run_results = get_artifact(project.project_root, "target", "run_results.json")
        assert run_results["args"]["vars"] == {
            "DBT_ENV_SECRET_simple": "*****",
            "unused": "def",
        }

        log_events = get_logging_events(log_output=output, event_name="StateCheckVarsHash")
        assert len(log_events) == 1
        assert (
            log_events[0]["data"]["vars"] == "{'DBT_ENV_SECRET_simple': '*****', 'unused': 'def'}"
        )

    def test__exception_scrubbing(self, project):
        results, output = run_dbt_and_capture(
            [
                "--debug",
                "--log-format",
                "json",
                "run",
                "--vars",
                "{DBT_ENV_SECRET_unused: abc, unused: def}",
            ],
            False,
        )
        assert len(results) == 1

        log_events = get_logging_events(log_output=output, event_name="CatchableExceptionOnRun")
        assert len(log_events) == 1
        assert (
            '{\n      "DBT_ENV_SECRET_unused": "*****",\n      "unused": "def"\n  }'
            in log_events[0]["info"]["msg"]
        )
