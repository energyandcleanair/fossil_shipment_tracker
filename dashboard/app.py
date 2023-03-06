import os.path

import pandas as pd
import plotly.express as px
import dash
import diskcache
import requests
from dash import dcc
from dash import html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
from dash import DiskcacheManager, CeleryManager, Input, Output, html
from dash.exceptions import PreventUpdate

from server import app, background_callback_manager, cache
from counter import layout as counter_layout
from voyages import layout as voyages_layout

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

# the styles for the main content position it to the right of the sidebar and
# add some padding.
CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
    "min-height": "100%",
    # "background-color": "red",
}

LAYOUT_STYLE = {
    # "background-color": "blue",
    "height": "100vh",
    "display": "flex",
    "flexDirection": "column",
}

sidebar = html.Div(
    [
        html.H3("Russia Fossil Tracker"),
        html.Hr(),
        # html.P("A simple sidebar layout with navigation links", className="lead"),
        dbc.Nav(
            [
                dbc.NavLink("Counter", href="/", active="exact"),
                dbc.NavLink("Shipments", href="/shipments", active="exact"),
                # dbc.NavLink("Page 2", href="/page-2", active="exact"),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style=SIDEBAR_STYLE,
)

content = html.Div(id="page-content", style=CONTENT_STYLE)
shared = [
    dcc.Interval(id="interval-component", interval=500000, n_intervals=0),  # in milliseconds
    dcc.Store(id="counter"),
    dcc.Store(id="counter-rolled"),
    dcc.Store(id="voyages"),
    html.Div(id="dummy_div"),
]
app.layout = dbc.Container(
    [dcc.Location(id="url"), sidebar, content, *shared],
    style=LAYOUT_STYLE,
    fluid=True,
    # die ganze app steckt in einem flex container; flex richtung ist column (standard); die row mit dem hauptinhalt bekommt flew grow
)


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname == "/" or pathname == "/counter":
        return counter_layout
    elif pathname == "/shipments":
        return voyages_layout
    # If the user tries to reach a different page, return a 404 message
    return html.Div(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ],
        className="p-3 bg-light rounded-3",
    )


# Define the callback function that loads the data from the API
@dash.callback(
    output=Output("counter", "data"),
    inputs=[Input("interval-component", "n_intervals")],
    background=True,
    manager=background_callback_manager,
)
def load_counter(n):
    # TODO Lohit: this is using a local cache
    # Wanting to use a redis cache
    # Like shown here:https://dash.plotly.com/background-callback-caching
    # Should probably use Google Cloud Memorystrore
    if n == 0:
        cached_data = cache.get("counter")
        if cached_data is not None:
            return cached_data

        print("=== loading data ===")
        file = "counter.csv"
        if not os.path.exists(file):
            storage_options = {"User-Agent": "Mozilla/5.0"}
            url = "https://api.russiafossiltracker.com/v0/counter?date_from=2022-01-01&format=csv"
            counter = pd.read_csv(url, storage_options=storage_options)
            counter.to_csv(file, index=False)
        else:
            counter = pd.read_csv("counter.csv")

        print("=== loading data done ===")
        data = counter.to_json(date_format="iso", orient="split")
        cache.set("counter", data)
        return data
    else:
        raise PreventUpdate


@dash.callback(
    output=Output("voyages", "data"),
    inputs=[Input("interval-component", "n_intervals")],
    background=True,
    manager=background_callback_manager,
)
def load_voyages(n):
    if n == 0:
        # TODO Lohit: this is using a local cache
        # Wanting to use a redis cache
        # Like shown here:https://dash.plotly.com/background-callback-caching
        # Should probably use Google Cloud Memorystrore
        cached_data = cache.get("voyages")
        if cached_data is not None:
            return cached_data

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
