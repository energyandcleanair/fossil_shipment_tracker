from itertools import chain
import pandas as pd

from ..voyage import VoyageResource
from ..kpler_trade import KplerTradeResource

# voyage: trade
KPLER_PARAMS_RENAMED = {"commodity": "commodity_equivalent"}

KPLER_COMMODITY_FILTER_CONVERSION = {
    "crude_oil": ["crude_oil", "crude_oil_espo", "crude_oil_urals"]
}

# voyage: trade
KPLER_COLUMNS_RENAMED = {
    "departure_date": "origin_date",
    "commodity_group": "commodity_equivalent",
    "commodity_group_name": "commodity_equivalent_name",
}


def get_voyages(params, aggregate_by, use_kpler=False):
    if use_kpler:
        return get_voyages_kpler(params, aggregate_by)
    else:
        return get_voyages_mt(params, aggregate_by)


def get_voyages_kpler(params, aggregate_by):
    params_kpler = params.copy()

    for voyage_name, trade_name in KPLER_PARAMS_RENAMED.items():
        if voyage_name in params_kpler:
            params_kpler[trade_name] = params_kpler[voyage_name]
            params_kpler[voyage_name] = None

    params_kpler["commodity_equivalent"] = list(
        chain.from_iterable(
            [
                KPLER_COMMODITY_FILTER_CONVERSION.get(val, [val])
                for val in params_kpler["commodity_equivalent"]
            ]
        )
    )

    params_kpler["aggregate_by"] = [KPLER_COLUMNS_RENAMED.get(col, col) for col in aggregate_by]

    response = KplerTradeResource().get_from_params(params_kpler)
    if response.status_code != 200:
        return pd.DataFrame()

    data = pd.DataFrame(response.json["data"])

    for voyage_name, trade_name in KPLER_COLUMNS_RENAMED.items():
        if trade_name in data.columns:
            data[voyage_name] = data[trade_name]
            data[trade_name] = None

    return data


def get_voyages_mt(params, aggregate_by):
    params_voyages = params.copy()
    params_voyages["aggregate_by"] = aggregate_by

    response = VoyageResource().get_from_params(params_voyages)
    if response.status_code != 200:
        return pd.DataFrame()

    return pd.DataFrame(response.json["data"])
