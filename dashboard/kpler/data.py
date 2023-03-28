import pandas as pd
import dash
import requests
from decouple import config
from dash import Input, Output, html, State
from dash.exceptions import PreventUpdate

from server import app, cache
from . import COUNTRY_GLOBAL
from . import FACET_NONE
from . import COMMODITY_ALL
from .utils import to_list, roll_average_kpler

"""
We create several level of kpler data.
Not all parameter changes require a new data query to the API, or roll-averaging.
"""


# perform expensive computations in this "global store"
# these computations are cached in a globally available
# redis memory store which is available across processes
# and for all time.
@cache.memoize()
def get_kpler0(origin_iso2, origin_type, destination_iso2, destination_type, commodity):
    # simulate expensive query
    print("=== loading kpler ===")
    columns = [
        "origin_name",
        "destination_name",
        "destination_region",
        "date",
        "product",
        "product_group",
        "product_family",
        "commodity_equivalent",
        "value_tonne",
        "value_eur",
        "value_usd",
    ]
    params = {
        "origin_iso2": ",".join(to_list(origin_iso2)),
        "origin_type": origin_type,
        "destination_type": destination_type,
        "api_key": config("API_KEY"),
        "select": ",".join(columns),
    }

    if COUNTRY_GLOBAL not in to_list(destination_iso2):
        params["destination_iso2"] = ",".join(to_list(destination_iso2))

    if COMMODITY_ALL not in to_list(commodity):
        params["commodity"] = ",".join(to_list(commodity))

    url = "https://api.russiafossiltracker.com/v1/kpler_flow"
    r = requests.get(url, params=params)
    data = r.json()
    print("=== done ===")
    return data.get("data")


@dash.callback(
    output=Output("kpler0", "data"),
    inputs=[
        State("kpler-origin-country", "value"),
        State("kpler-origin-type", "value"),
        State("kpler-destination-country", "value"),
        State("kpler-destination-type", "value"),
        State("kpler-commodity", "value"),
        Input("kpler-refresh", "n_clicks"),
    ],
)
def load_kpler0(origin_iso2, origin_type, destination_iso2, destination_type, commodity, n):
    if n is not None:
        return get_kpler0(origin_iso2, origin_type, destination_iso2, destination_type, commodity)
    else:
        raise PreventUpdate


@dash.callback(
    output=Output("kpler1", "data"),
    inputs=[Input("kpler0", "data"), Input("kpler-rolling-days", "value")],
)
def load_kpler1(kpler0, rolling_days):
    if kpler0 is None:
        raise PreventUpdate
    kpler1 = pd.DataFrame(kpler0)
    kpler1 = roll_average_kpler(kpler1, rolling_days)
    return kpler1.to_json(date_format="iso", orient="split")


@app.callback(
    output=Output("kpler2", "data"),
    inputs=[
        Input("kpler1", "data"),
        Input("colour-by", "value"),
        Input("facet", "value"),
    ],
)
def load_kpler2(json_data, colour_by, facet):
    if facet == FACET_NONE:
        facet = None
    if json_data is None:
        raise PreventUpdate
    df = pd.read_json(json_data, orient="split")
    aggregate_by = list(set(["date"] + [colour_by] + [facet]))
    aggregate_by = [x for x in aggregate_by if x is not None]

    value_cols = [x for x in df.columns if x.startswith("value_")]
    df = df.groupby(aggregate_by)[value_cols].sum().reset_index()

    # Group largest colours together
    largest = df.groupby(colour_by)[value_cols].sum().nlargest(9, columns=value_cols[0]).index
    df.loc[~df[colour_by].isin(largest), colour_by] = "Other"
    df = df.groupby(aggregate_by)[value_cols].sum().reset_index()

    # Remove all first rows of df until the first date with a non-zero value
    min_date = df.loc[(df[value_cols] > 0).apply(any, axis=1)]["date"].min()
    df = df[df["date"] >= min_date]
    return df.to_json(date_format="iso", orient="split")
