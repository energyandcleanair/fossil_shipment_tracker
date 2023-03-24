import pandas as pd
import plotly.express as px
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State, dcc
from dash.exceptions import PreventUpdate
from server import app, background_callback_manager, cache
from utils import palette

from . import COUNTRY_GLOBAL
from . import FACET_NONE
from . import units
from . import laundromat_iso2s, pcc_iso2s
from .utils import roll_average_kpler


@app.callback(
    Output("download-kpler0", "data"),
    State("kpler0", "data"),
    Input("btn-download-kpler0", "n_clicks"),
    prevent_initial_call=True,
)
def download_kpler0(data, n):
    df = pd.DataFrame(data)
    return dcc.send_data_frame(df.to_csv, "kpler_raw.csv")


@app.callback(
    Output("download-kpler2", "data"),
    State("kpler0", "data"),
    Input("btn-download-kpler2", "n_clicks"),
    prevent_initial_call=True,
)
def download_kpler0(data, n):
    df = pd.DataFrame(data)
    return dcc.send_data_frame(df.to_csv, "kpler_processed.csv")
