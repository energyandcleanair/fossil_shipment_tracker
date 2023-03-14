import pandas as pd
import plotly.express as px
import dash
import diskcache
import requests
from dash import dcc
from dash import html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State
from dash.exceptions import PreventUpdate
from server import app, background_callback_manager, cache

from . import COUNTRY_GLOBAL
from . import FACET_NONE


@dash.callback(
    output=Output("kpler_level0", "data"),
    inputs=[
        Input("origin_iso2", "value"),
        Input("origin_type", "value"),
        Input("destination_type", "value"),
        Input("destination_type", "value"),
    ],
    # background=True,
    manager=background_callback_manager,
)
def load_kpler_level0(n):
    # TODO Lohit: this is using a local cache
    # Wanting to use a redis cache
    # Like shown here:https://dash.plotly.com/background-callback-caching
    # Should probably use Google Cloud Memorystrore
    if n == 1:
        print("=== loading counter ===")
        cached_data = cache.get("counter")
        if cached_data is not None:
            print("found cache: rows = %d" % (len(cached_data)))
            return cached_data

        print("=== loading data ===")
        file = "counter.csv"
        # if not os.path.exists(file):
        #     storage_options = {"User-Agent": "Mozilla/5.0"}
        #     url = "https://api.russiafossiltracker.com/v0/counter?date_from=2022-01-01&format=csv"
        #     counter = pd.read_csv(url, storage_options=storage_options)
        #     counter.to_csv(file, index=False)
        # else:
        counter = pd.read_csv("counter.csv")

        print("=== loading data done ===")
        data = counter.to_json(date_format="iso", orient="split")
        cache.set("counter", data)
        return data
    else:
        raise PreventUpdate


@dash.callback(
    output=Output("kpler_level1", "data"),
    inputs=[
        Input("kpler_level0", "value"),
        Input("origin_zone", "value"),
        Input("destination_iso2", "value"),
        Input("destination_zone", "value"),
        Input("product", "value"),
    ],
    # background=True,
    manager=background_callback_manager,
)
def load_kpler_level1(kpler_level0, origin_zone, destination_iso2, destination_zone, product):
    # TODO Lohit: this is using a local cache
    # Wanting to use a redis cache
    # Like shown here:https://dash.plotly.com/background-callback-caching
    # Should probably use Google Cloud Memorystrore
    if n == 1:
        print("=== loading counter ===")
        cached_data = cache.get("counter")
        if cached_data is not None:
            print("found cache: rows = %d" % (len(cached_data)))
            return cached_data

        print("=== loading data ===")
        file = "counter.csv"
        # if not os.path.exists(file):
        #     storage_options = {"User-Agent": "Mozilla/5.0"}
        #     url = "https://api.russiafossiltracker.com/v0/counter?date_from=2022-01-01&format=csv"
        #     counter = pd.read_csv(url, storage_options=storage_options)
        #     counter.to_csv(file, index=False)
        # else:
        counter = pd.read_csv("counter.csv")

        print("=== loading data done ===")
        data = counter.to_json(date_format="iso", orient="split")
        cache.set("counter", data)
        return data
    else:
        raise PreventUpdate
