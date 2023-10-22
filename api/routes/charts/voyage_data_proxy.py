from itertools import chain
import pandas as pd

from ..voyage import VoyageResource
from ..kpler_trade import KplerTradeResource

# voyage: trade
KPLER_PARAMS_RENAMED = {"commodity": "commodity_equivalent"}

# voyage: trade
KPLER_COMMODITY_FILTER_CONVERSION = {
    "crude_oil": ["crude_oil", "crude_oil_espo", "crude_oil_urals"]
}

# voyage: trade
KPLER_COLUMNS_RENAMED = {
    "departure_date_from": "origin_date_from",
    "departure_date": "origin_date",
    "commodity": "commodity_equivalent",
    "commodity_name": "commodity_equivalent_name",
    "commodity_group": "commodity_equivalent",
    "commodity_group_name": "commodity_equivalent_name",
}

# voyage: trade
KPLER_COLUMNS_COPIES = {"arrival_detected_date": "destination_date"}


def get_kpler_name(name):
    if name in KPLER_COLUMNS_COPIES:
        return KPLER_COLUMNS_COPIES[name]
    if name in KPLER_COLUMNS_RENAMED:
        return KPLER_COLUMNS_RENAMED[name]
    return name


def get_voyages(params, use_kpler=False):
    if use_kpler:
        return get_voyages_kpler(params)
    else:
        return get_voyages_mt(params)


def get_voyages_kpler(params):
    params_kpler = params.copy()

    for voyage_name, trade_name in KPLER_PARAMS_RENAMED.items():
        if voyage_name in params_kpler:
            params_kpler[trade_name] = params_kpler[voyage_name]
            params_kpler[voyage_name] = None

    if params_kpler["commodity"]:
        params_kpler["commodity_equivalent"] = list(
            chain.from_iterable(
                [
                    KPLER_COMMODITY_FILTER_CONVERSION.get(val, [val])
                    for val in params_kpler["commodity"]
                ]
            )
        )

    if params_kpler["aggregate_by"]:
        params_kpler["aggregate_by"] = [get_kpler_name(col) for col in params_kpler["aggregate_by"]]

    if params_kpler["select"]:
        params_kpler["select"] = [get_kpler_name(col) for col in params_kpler["select"]]

    response = KplerTradeResource().get_from_params(params_kpler)
    if response.status_code != 200:
        return pd.DataFrame()

    data = pd.DataFrame(response.json["data"])

    for voyage_name, trade_name in KPLER_COLUMNS_COPIES.items():
        if trade_name in data.columns:
            data[voyage_name] = data[trade_name]

    for voyage_name, trade_name in KPLER_COLUMNS_RENAMED.items():
        if trade_name in data.columns:
            data[voyage_name] = data[trade_name]

    for _, trade_name in KPLER_COLUMNS_RENAMED.items():
        if trade_name in data.columns:
            data.drop(trade_name, axis=1)

    return data


def get_voyages_mt(params):
    params_voyages = params.copy()

    response = VoyageResource().get_from_params(params_voyages)
    if response.status_code != 200:
        return pd.DataFrame()

    return pd.DataFrame(response.json["data"])
