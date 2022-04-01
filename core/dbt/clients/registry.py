import functools
import requests
from dbt.events.functions import fire_event
from dbt.events.types import (
    RegistryProgressMakingGETRequest,
    RegistryProgressGETResponse,
    RegistryIndexProgressMakingGETRequest,
    RegistryIndexProgressGETResponse,
)
from dbt.utils import memoized, _connection_exception_retry as connection_exception_retry
from dbt import deprecations
import os

if os.getenv("DBT_PACKAGE_HUB_URL"):
    DEFAULT_REGISTRY_BASE_URL = os.getenv("DBT_PACKAGE_HUB_URL")
else:
    DEFAULT_REGISTRY_BASE_URL = "https://hub.getdbt.com/"


def _get_url(name, registry_base_url=None):
    if registry_base_url is None:
        registry_base_url = DEFAULT_REGISTRY_BASE_URL
    url = "api/v1/{}.json".format(name)

    return "{}{}".format(registry_base_url, url)


def _get_with_retries(package_name, registry_base_url=None):
    get_fn = functools.partial(_get_cached, package_name, registry_base_url)
    return connection_exception_retry(get_fn, 5)


def _get(package_name, registry_base_url=None):
    url = _get_url(package_name, registry_base_url)
    fire_event(RegistryProgressMakingGETRequest(url=url))
    resp = requests.get(url, timeout=30)
    fire_event(RegistryProgressGETResponse(url=url, resp_code=resp.status_code))
    resp.raise_for_status()

    # The response should always be a dictionary.  Anything else is unexpected, raise error.
    # Raising this error will cause this function to retry (if called within _get_with_retries)
    # and hopefully get a valid response.  This seems to happen when there's an issue with the Hub.
    # Since we control what we expect the HUB to return, this is safe.
    # See https://github.com/dbt-labs/dbt-core/issues/4577
    # and https://github.com/dbt-labs/dbt-core/issues/4849
    response = resp.json()

    if not isinstance(response, dict):  # This will also catch Nonetype
        error_msg = (
            f"Request error: The response type of {type(response)} is not valid: {resp.text}"
        )
        raise requests.exceptions.ContentDecodingError(error_msg, response=resp)

    expected_keys = ["name", "versions"]
    expected_version_keys = ["name", "packages", "downloads"]
    if not set(expected_keys).issubset(response) and not set(expected_version_keys).issubset(
        response["versions"]
    ):
        error_msg = f"Request error: Expected the response to contain keys {expected_keys} but one or more are missing: {resp.text}"
        raise requests.exceptions.ContentDecodingError(error_msg, response=resp)

    # Either redirectnamespace or redirectname in the JSON response indicate a redirect
    # redirectnamespace redirects based on package ownership
    # redirectname redirects based on package name
    # Both can be present at the same time, or neither. Fails gracefully to old name
    if ("redirectnamespace" in response) or ("redirectname" in response):

        if ("redirectnamespace" in response) and response["redirectnamespace"] is not None:
            use_namespace = response["redirectnamespace"]
        else:
            use_namespace = response["namespace"]

        if ("redirectname" in response) and response["redirectname"] is not None:
            use_name = response["redirectname"]
        else:
            use_name = response["name"]

        new_nwo = use_namespace + "/" + use_name
        deprecations.warn("package-redirect", old_name=package_name, new_name=new_nwo)

    return response


_get_cached = memoized(_get)


def package(package_name, registry_base_url=None):
    # returns a dictionary of metadata for all versions of a package
    response = _get_with_retries(package_name, registry_base_url)
    return response["versions"]


def package_version(package_name, version, registry_base_url=None):
    # returns the metadata of a specific version of a package
    response = package(package_name, registry_base_url)
    return response[version]


def get_available_versions(package_name):
    # returns a list of all available versions of a package
    response = package(package_name)
    return list(response)


def _get_index(registry_base_url=None):

    url = _get_url("index", registry_base_url)
    fire_event(RegistryIndexProgressMakingGETRequest(url=url))
    resp = requests.get(url, timeout=30)
    fire_event(RegistryIndexProgressGETResponse(url=url, resp_code=resp.status_code))
    resp.raise_for_status()

    # The response should be a list.  Anything else is unexpected, raise an error.
    # Raising this error will cause this function to retry and hopefully get a valid response.

    response = resp.json()

    if not isinstance(response, list):  # This will also catch Nonetype
        error_msg = (
            f"Request error: The response type of {type(response)} is not valid: {resp.text}"
        )
        raise requests.exceptions.ContentDecodingError(error_msg, response=resp)

    return response


def index(registry_base_url=None):
    # this returns a list of all packages on the Hub
    get_index_fn = functools.partial(_get_index, registry_base_url)
    return connection_exception_retry(get_index_fn, 5)


index_cached = memoized(index)
