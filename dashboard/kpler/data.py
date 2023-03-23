import os
import pandas as pd
import plotly.express as px
import dash
import diskcache
import requests
from decouple import config
from dash import dcc
from dash import html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State
from dash.exceptions import PreventUpdate
from server import app, background_callback_manager, cache

from . import COUNTRY_GLOBAL
from . import FACET_NONE
from . import units
from . import refreshing
from .utils import to_list, roll_average_kpler

"""
We create several level of kpler data.
Not all parameter changes require a new data query to the API.
"""


@dash.callback(
    output=Output("kpler0", "data"),
    inputs=[
        State("kpler-origin-country", "value"),
        State("kpler-origin-type", "value"),
        State("kpler-destination-country", "value"),
        State("kpler-destination-type", "value"),
        Input("kpler-refresh", "n_clicks"),
    ],
    # background=True,
    manager=background_callback_manager,
)
def load_kpler0(origin_iso2, origin_type, destination_iso2, destination_type, n):
    if n is not None:
        print("=== loading kpler ===")
        # cached_data = cache.get("counter")
        # if cached_data is not None:
        #     print("found cache: rows = %d" % (len(cached_data)))
        #     return cached_data

        print("=== loading data ===")

        params = {
            "origin_iso2": ",".join(to_list(origin_iso2)),
            "origin_by": origin_type,
            "destination_by": destination_type,
            "api_key": config("API_KEY"),
        }

        if COUNTRY_GLOBAL not in to_list(destination_iso2):
            params["destination_iso2"] = ",".join(to_list(destination_iso2))

        url = "https://api.russiafossiltracker.com/v1/kpler_flow"
        r = requests.get(url, params=params)
        data = r.json()
        # TODO cache here??
        return data.get("data")
    else:
        raise PreventUpdate


@dash.callback(
    output=Output("kpler1", "data"),
    inputs=[
        Input("kpler0", "data"),
        Input("kpler-rolling-days", "value"),
        # Input("destination_iso2", "value"),
        # Input("destination_zone", "value"),
        # Input("product", "value"),
    ],
    # background=True,
    manager=background_callback_manager,
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
    manager=background_callback_manager,
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
