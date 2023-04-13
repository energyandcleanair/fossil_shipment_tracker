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

from server import app, cache

# from counter import layout as counter_layout, chart_settings as counter_chart_settings
# from voyages import layout as voyages_layout, chart_settings as voyages_chart_settings

import kpler
from kpler import layout as kpler_layout, chart_settings as kpler_chart_settings
from kpler import store as kpler_store

import insurance
from insurance import layout as insurance_layout, chart_settings as insurance_chart_settings
from insurance import store as insurance_store

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "24rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
    "overflow-y": "scroll",
}

# the styles for the main content position it to the right of the sidebar and
# add some padding.
CONTENT_STYLE = {
    "margin-left": "24rem",
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
        html.Div("Russia Fossil Tracker", className="dashboard-title"),
        html.Div("Dashboard", className="dashboard-subtitle"),
        html.Hr(),
        dbc.Nav(
            [
                # dbc.NavLink("Counter", href="/", active="exact"),
                dbc.NavLink("Kpler", href="/kpler", active="exact"),
                dbc.NavLink("Insurance", href="/insurance", active="exact"),
            ],
            vertical=True,
            pills=True,
        ),
        dbc.Container(html.Div(id="chart-settings")),
    ],
    style=SIDEBAR_STYLE,
)

content = html.Div(id="page-content", style=CONTENT_STYLE)
shared = (
    [
        dcc.Interval(id="interval-component", interval=1000, n_intervals=1),  # in milliseconds
        html.Div(id="dummy_div"),
    ]
    + kpler_store
    + insurance_store
)

app.layout = dbc.Container(
    [dcc.Location(id="url"), sidebar, content, *shared],
    style=LAYOUT_STYLE,
    fluid=True,
    # die ganze app steckt in einem flex container; flex richtung ist column (standard); die row mit dem hauptinhalt bekommt flew grow
)
# Expose server
server = app.server


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    # if pathname == "/counter":
    #     return counter_layout
    # elif pathname == "/shipments":
    #     return voyages_layout
    if pathname == "/":
        return dcc.Location(pathname="/kpler", id="someid_doesnt_matter")
    elif pathname == "/kpler":
        return kpler_layout
    elif pathname == "/insurance":
        return insurance_layout
    # If the user tries to reach a different page, return a 404 message
    return html.Div(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ],
        className="p-3 bg-light rounded-3",
    )


@app.callback(Output("chart-settings", "children"), [Input("url", "pathname")])
def render_chart_setting(pathname):
    # if pathname == "/" or pathname == "/counter":
    #     return counter_chart_settings
    # elif pathname == "/shipments":
    #     return voyages_chart_settings
    if pathname == "/" or pathname == "/kpler":
        return kpler_chart_settings
    elif pathname == "/insurance":
        return insurance_chart_settings
    return None
