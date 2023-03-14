import pandas as pd
import dash
import diskcache
import os
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State
from dash.exceptions import PreventUpdate
from server import app, background_callback_manager, cache


@dash.callback(
    output=Output("voyages", "data"),
    inputs=[Input("interval-component", "n_intervals")],
    # background=True,
    # manager=background_callback_manager,
)
def load_voyages(n):
    if n == 1:
        # TODO Lohit: this is using a local cache
        # Wanting to use a redis cache
        # Like shown here:https://dash.plotly.com/background-callback-caching
        # Should probably use Google Cloud Memorystrore
        # cached_data = cache.get("voyages")
        # if cached_data is not None:
        #     return cached_data

        print("=== loading voyages ===")
        file = "voyages.csv"
        if not os.path.exists(file):
            storage_options = {"User-Agent": "Mozilla/5.0"}
            url = "https://api.russiafossiltracker.com/v0/voyage?date_from=2022-01-01&format=csv&select_set=light"
            voyages = pd.read_csv(url, storage_options=storage_options)
            voyages.to_csv(file, index=False)
        else:
            voyages = pd.read_csv("voyages.csv")

        data = voyages.to_json(date_format="iso", orient="split")
        cache.set("voyages", data)
        return data
    else:
        raise PreventUpdate
