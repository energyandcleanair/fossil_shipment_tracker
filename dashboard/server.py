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

launch_uid = "RFT"

cache = diskcache.Cache("./cache")
background_callback_manager = DiskcacheManager(cache, cache_by=[lambda: launch_uid], expire=6000)

# Create a Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    background_callback_manager=background_callback_manager,
    suppress_callback_exceptions=True,
)
