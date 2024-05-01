from warnings import warn
import pandas as pd
import requests
from base.env import get_env

from base.logger import logger

API_BASE = get_env("FOSSIL_SHIPMENT_TRACKER_API_URL")
API_KEY = get_env("API_KEY")

DEFAULT_ARGS = {
    "format": "json",
    "download": False,
    "nest_in_data": False,
}


def _build_log_url(url):
    return url.replace(API_KEY, "***API_KEY***")


def _build_exception(response):
    url_without_key = _build_log_url(response.url)
    return Exception(
        f"Error fetching {url_without_key}: {response.status_code} - {response.reason}"
    )


def _replace_list_params_with_csv(params):
    for k, v in params.items():
        if isinstance(v, list):
            params[k] = ",".join(v)
    return params


def _make_request_with_retries(url, params):
    params_for_logs = {k: (v if k != "api_key" else "***API_KEY***") for k, v in params.items()}
    logger.info(f"Fetching {url} with params: {params_for_logs}")
    for _ in range(3):
        try:
            resp = requests.get(url, params=_replace_list_params_with_csv(params))
            if resp.status_code not in [200, 204]:
                raise _build_exception(resp)
            return pd.DataFrame(resp.json())
        except Exception as e:
            logger.error(f"Error fetching {_build_log_url(url)}: {e}")
    raise Exception(f"Failed to fetch {url} after 3 retries")


def get_voyages(**kwargs):
    return _make_request_with_retries(
        f"{API_BASE}/v0/voyage",
        params={**kwargs, **DEFAULT_ARGS, "bypass_maintenance": True},
    )


def get_overland(**kwargs):
    return _make_request_with_retries(
        f"{API_BASE}/v0/overland",
        params={**kwargs, **DEFAULT_ARGS, "bypass_maintenance": True},
    )


def get_kpler_flows(**kwargs):
    warn(
        "This function is deprecated. Use get_kpler_trades instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _make_request_with_retries(
        f"{API_BASE}/v1/kpler_flow",
        params={**kwargs, **DEFAULT_ARGS, "api_key": API_KEY},
    )


def get_kpler_trades(**kwargs):
    return _make_request_with_retries(
        f"{API_BASE}/v1/kpler_trade",
        params={**kwargs, **DEFAULT_ARGS, "api_key": API_KEY},
    )


def get_counter(**kwargs):
    return _make_request_with_retries(
        f"{API_BASE}/v0/counter",
        params={**kwargs, **DEFAULT_ARGS},
    )
