import pandas as pd
import plotly.express as px
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State, dcc
from dash.exceptions import PreventUpdate
from server import app, cache
from utils import palette
from . import FACET_NONE
from .data import get_insurance_full


@app.callback(
    Output("download-insurance0", "data"),
    State("insurance0", "data"),
    Input("btn-download-insurance0", "n_clicks"),
    prevent_initial_call=True,
)
def download_insurance0(data, n):
    df = pd.DataFrame(data)
    return dcc.send_data_frame(df.to_csv, "insurance_raw.csv")


@app.callback(
    Output("download-insurance1", "data"),
    Input("btn-download-insurance1", "n_clicks"),
    State("insurance-origin-country", "value"),
    State("insurance-destination-country", "value"),
    State("insurance-commodity", "value"),
    # State("colour-by", "value"),
    State("facet", "value"),
    State("insurance-rolling-days", "value"),
    # Chart specific
    State("unit", "value"),
    State("insurance-chart-type", "value"),
    prevent_initial_call=True,
)
def download_insurance1(
    n,
    origin_iso2,
    destination_iso2,
    commodity,
    # colour_by,
    facet,
    rolling_days,
    unit_id,
    chart_type,
):
    if facet == FACET_NONE:
        facet = None
    if chart_type == "bar":
        rolling_days = 1

    df = get_insurance_full(
        origin_iso2,
        destination_iso2,
        commodity,
        colour_by,
        facet,
        rolling_days,
    )
    return dcc.send_data_frame(df.to_csv, "insurance_processed.csv")
