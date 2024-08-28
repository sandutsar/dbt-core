import pytest
import yaml

import dbt_common
from dbt import deprecations
from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file
from dbt_common.exceptions import EventCompilationError
from tests.functional.deprecations.fixtures import (
    bad_name_yaml,
    models_trivial__model_sql,
)


class TestConfigPathDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "data-paths": ["data"],
            "log-path": "customlogs",
            "target-path": "customtarget",
        }

    def test_data_path(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["debug"])
        expected = {
            "project-config-data-paths",
            "project-config-log-path",
            "project-config-target-path",
        }
        assert expected == deprecations.active_deprecations

    def test_data_path_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt_common.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "debug"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `data-paths` config has been renamed"
        assert expected_msg in exc_str


class TestPackageInstallPathDeprecation:
    @pytest.fixture(scope="class")
    def models_trivial(self):
        return {"model.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "clean-targets": ["dbt_modules"]}

    def test_package_path(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["clean"])
        expected = {"install-packages-path"}
        assert expected == deprecations.active_deprecations

    def test_package_path_not_set(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt_common.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "clean"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "path has changed from `dbt_modules` to `dbt_packages`."
        assert expected_msg in exc_str


class TestPackageRedirectDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"already_exists.sql": models_trivial__model_sql}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "fishtown-analytics/dbt_utils", "version": "0.7.0"}]}

    def test_package_redirect(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["deps"])
        expected = {"package-redirect"}
        assert expected == deprecations.active_deprecations

    # if this test comes before test_package_redirect it will raise an exception as expected
    def test_package_redirect_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt_common.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "deps"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "The `fishtown-analytics/dbt_utils` package is deprecated in favor of `dbt-labs/dbt_utils`"
        assert expected_msg in exc_str


class TestExposureNameDeprecation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": models_trivial__model_sql, "bad_name.yml": bad_name_yaml}

    def test_exposure_name(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        run_dbt(["parse"])
        expected = {"exposure-name"}
        assert expected == deprecations.active_deprecations

    def test_exposure_name_fail(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()
        with pytest.raises(dbt_common.exceptions.CompilationError) as exc:
            run_dbt(["--warn-error", "--no-partial-parse", "parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "Starting in v1.3, the 'name' of an exposure should contain only letters, numbers, and underscores."
        assert expected_msg in exc_str


class TestProjectFlagsMovedDeprecation:
    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {
            "config": {"send_anonymous_usage_stats": False},
        }

    @pytest.fixture(scope="class")
    def dbt_project_yml(self, project_root, project_config_update):
        project_config = {
            "name": "test",
            "profile": "test",
        }
        write_file(yaml.safe_dump(project_config), project_root, "dbt_project.yml")
        return project_config

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as fun"}

    def test_profile_config_deprecation(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()

        _, logs = run_dbt_and_capture(["parse"])

        assert (
            "User config should be moved from the 'config' key in profiles.yml to the 'flags' key in dbt_project.yml."
            in logs
        )
        assert deprecations.active_deprecations == {"project-flags-moved"}


class TestProjectFlagsMovedDeprecationQuiet(TestProjectFlagsMovedDeprecation):
    def test_profile_config_deprecation(self, project):
        deprecations.reset_deprecations()
        assert deprecations.active_deprecations == set()

        _, logs = run_dbt_and_capture(["--quiet", "parse"])

        assert (
            "User config should be moved from the 'config' key in profiles.yml to the 'flags' key in dbt_project.yml."
            not in logs
        )
        assert deprecations.active_deprecations == {"project-flags-moved"}


class TestProjectFlagsMovedDeprecationWarnErrorOptions(TestProjectFlagsMovedDeprecation):
    def test_profile_config_deprecation(self, project):
        deprecations.reset_deprecations()
        with pytest.raises(EventCompilationError):
            run_dbt(["--warn-error-options", "{'include': 'all'}", "parse"])

        with pytest.raises(EventCompilationError):
            run_dbt(
                ["--warn-error-options", "{'include': ['ProjectFlagsMovedDeprecation']}", "parse"]
            )

        _, logs = run_dbt_and_capture(
            ["--warn-error-options", "{'silence': ['ProjectFlagsMovedDeprecation']}", "parse"]
        )
        assert (
            "User config should be moved from the 'config' key in profiles.yml to the 'flags' key in dbt_project.yml."
            not in logs
        )
