import pandas as pd
import plotly.express as px
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State, dcc
from dash.exceptions import PreventUpdate
from server import app, cache
from utils import palette
from . import FACET_NONE
from .data import get_kpler_full, get_kpler0


@app.callback(
    Output("download-kpler0", "data"),
    Input("btn-download-kpler0", "n_clicks"),
    State("kpler-origin-country", "value"),
    State("kpler-origin-type", "value"),
    State("kpler-destination-country", "value"),
    State("kpler-destination-type", "value"),
    State("kpler-commodity", "value"),
    prevent_initial_call=True,
)
def download_kpler0(n, origin_iso2, origin_type, destination_iso2, destination_type, commodity):

    kpler0 = get_kpler0(origin_iso2, origin_type, destination_iso2, destination_type, commodity)
    df = pd.DataFrame(kpler0)
    return dcc.send_data_frame(df.to_csv, "kpler_raw.csv")


@app.callback(
    Output("download-kpler1", "data"),
    Input("btn-download-kpler1", "n_clicks"),
    State("kpler-origin-country", "value"),
    State("kpler-origin-type", "value"),
    State("kpler-destination-country", "value"),
    State("kpler-destination-type", "value"),
    State("kpler-commodity", "value"),
    State("colour-by", "value"),
    State("facet", "value"),
    State("kpler-rolling-days", "value"),
    # Chart specific
    State("unit", "value"),
    State("kpler-chart-type", "value"),
    State("kpler-top-n", "value"),
    prevent_initial_call=True,
)
def download_kpler1(
    n,
    origin_iso2,
    origin_type,
    destination_iso2,
    destination_type,
    commodity,
    colour_by,
    facet,
    rolling_days,
    unit_id,
    chart_type,
    top_n,
):
    if facet == FACET_NONE:
        facet = None
    if chart_type == "bar":
        rolling_days = 1

    df = get_kpler_full(
        origin_iso2,
        origin_type,
        destination_iso2,
        destination_type,
        commodity,
        colour_by,
        facet,
        rolling_days,
        top_n,
    )
    return dcc.send_data_frame(df.to_csv, "kpler_processed.csv")
