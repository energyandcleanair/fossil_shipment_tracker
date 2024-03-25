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


def build_exception(response):
    url_without_key = response.url.replace(API_KEY, "***API_KEY***")
    return Exception(
        f"Error fetching {url_without_key}: {response.status_code} - {response.reason}"
    )


def get_voyages(**kwargs):
    url = f"{API_BASE}/v0/voyage"

    args = {**kwargs, **DEFAULT_ARGS}
    logger.info(f"Fetching voyages with args: {args}")

    resp = requests.get(
        url,
        params=args,
    )

    if resp.status_code != 200:
        raise build_exception(resp)

    voyages_df = pd.DataFrame(resp.json())
    return voyages_df


def get_overland(**kwargs):
    url = f"{API_BASE}/v0/overland"

    args = {**kwargs, **DEFAULT_ARGS}
    logger.info(f"Fetching overland with args: {args}")

    resp = requests.get(
        url,
        params=args,
    )

    if resp.status_code != 200:
        raise build_exception(resp)

    overland_df = pd.DataFrame(resp.json())
    return overland_df


def get_counter(**kwargs):
    url = f"{API_BASE}/v0/counter"

    args = {**kwargs, **DEFAULT_ARGS}
    logger.info(f"Fetching counter with args: {args}")

    resp = requests.get(
        url,
        params=args,
    )

    if resp.status_code != 200:
        raise build_exception(resp)

    counter_df = pd.DataFrame(resp.json())
    return counter_df


def get_kpler_flows(**kwargs):
    warn(
        "This function is deprecated. Use get_kpler_trades instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    url = f"{API_BASE}/v1/kpler_flows"

    args = {**kwargs, **DEFAULT_ARGS, "api_key": API_KEY}
    logger.info(f"Fetching kpler_flows with args: {args}")

    resp = requests.get(
        url,
        params=args,
    )

    if resp.status_code != 200:
        raise build_exception(resp)

    kpler_flows_df = pd.DataFrame(resp.json())
    return kpler_flows_df


def get_kpler_trades(**kwargs):
    url = f"{API_BASE}/v1/kpler_trades"

    args = {**kwargs, **DEFAULT_ARGS, "api_key": API_KEY}
    logger.info(f"Fetching kpler_trades with args: {args}")

    resp = requests.get(
        url,
        params=args,
    )

    if resp.status_code != 200:
        raise build_exception(resp)

    kpler_trades_df = pd.DataFrame(resp.json())
    return kpler_trades_df
