import functools
import requests
from dbt.events.functions import fire_event
from dbt.events.types import RegistryProgressMakingGETRequest, RegistryProgressGETResponse
from dbt.utils import memoized, _connection_exception_retry as connection_exception_retry
from dbt import deprecations
import os

if os.getenv("DBT_PACKAGE_HUB_URL"):
    DEFAULT_REGISTRY_BASE_URL = os.getenv("DBT_PACKAGE_HUB_URL")
else:
    DEFAULT_REGISTRY_BASE_URL = "https://hub.getdbt.com/"


def _get_url(url, registry_base_url=None):
    if registry_base_url is None:
        registry_base_url = DEFAULT_REGISTRY_BASE_URL

    return "{}{}".format(registry_base_url, url)


def _get_with_retries(path, registry_base_url=None):
    get_fn = functools.partial(_get, path, registry_base_url)
    return connection_exception_retry(get_fn, 5)


def _get(path, registry_base_url=None):
    url = _get_url(path, registry_base_url)
    fire_event(RegistryProgressMakingGETRequest(url=url))
    resp = requests.get(url, timeout=30)
    fire_event(RegistryProgressGETResponse(url=url, resp_code=resp.status_code))
    resp.raise_for_status()

    # The response should either be a list or a dictionary.  Anything else is unexpected, raise error.
    # Raising this error will cause this function to retry (if called within _get_with_retries)
    # and hopefully get a valid response.  This seems to happen when there's an issue with the Hub.
    # Since we control what we expect the HUB to return, this is safe.
    # See https://github.com/dbt-labs/dbt-core/issues/4577
    # and https://github.com/dbt-labs/dbt-core/issues/4849
    json_response = resp.json()
    if (json_response is None) or (
        not isinstance(json_response, list) and not isinstance(json_response, dict)
    ):
        error_msg = f"Request error: The response is not valid json: {resp.text}"
        raise requests.exceptions.ContentDecodingError(error_msg, response=resp)
    return json_response


def index(registry_base_url=None):
    # this returns a list of all packages on the Hub
    return _get_with_retries("api/v1/index.json", registry_base_url)


index_cached = memoized(index)


def package(name, registry_base_url=None):
    # expect back a dictionary
    response = _get_with_retries("api/v1/{}.json".format(name), registry_base_url)

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
        deprecations.warn("package-redirect", old_name=name, new_name=new_nwo)

    return response


def package_version(name, version, registry_base_url=None):
    # expect back a dictionary
    return _get_with_retries("api/v1/{}/{}.json".format(name, version), registry_base_url)


def get_available_versions(name):
    response = package(name)
    return list(response["versions"])
