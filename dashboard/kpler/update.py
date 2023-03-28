import pandas as pd
import plotly.express as px
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State, dcc
from dash.exceptions import PreventUpdate
from server import app, cache
from utils import palette

from . import COUNTRY_GLOBAL
from . import FACET_NONE
from . import units
from . import laundromat_iso2s, pcc_iso2s
from .utils import roll_average_kpler


@app.callback(
    output=Output("kpler-origin-country", "value", allow_duplicate=True),
    inputs=[
        Input("kpler-origin-country-select-laundromat", "n_clicks"),
    ],
    allow_duplicate=True,
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_origin_laundromat(n_clicks):
    return laundromat_iso2s


@app.callback(
    output=Output("kpler-origin-country", "value", allow_duplicate=True),
    inputs=[
        Input("kpler-origin-country-select-russia", "n_clicks"),
    ],
    allow_duplicate=True,
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_origin_laundromat(n_clicks):
    return ["RU"]


@app.callback(
    output=Output("kpler-destination-country", "value", allow_duplicate=True),
    inputs=[
        Input("kpler-destination-country-select-laundromat", "n_clicks"),
    ],
    allow_duplicate=True,
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_origin_laundromat(n_clicks):
    return laundromat_iso2s


@app.callback(
    output=Output(
        "kpler-destination-country",
        "value",
        allow_duplicate=True,
    ),
    inputs=[
        Input("kpler-destination-country-select-pcc", "n_clicks"),
    ],
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_origin_pcc(n_clicks):
    return pcc_iso2s


@app.callback(
    output=Output("kpler-area-chart", "figure"),
    inputs=[
        Input("kpler2", "data"),
        Input("colour-by", "value"),
        Input("unit", "value"),
        Input("facet", "value"),
        Input("kpler-chart-type", "value"),
    ],
    suppress_callback_exceptions=True,
)
def update_chart(json_data, colour_by, unit_id, facet, chart_type):
    if facet == FACET_NONE:
        facet = None
    if json_data is None:
        raise PreventUpdate
    df = pd.read_json(json_data, orient="split")
    unit = units[unit_id]
    value = unit["column"]
    unit_str = unit["label"]
    unit_format = unit["format"]
    unit_scale = unit["scale"]
    df[value] = df[value] * unit_scale
    hovertemplate = f"%{{customdata[0]}}: %{{y:{unit_format}}} {unit_str}<extra></extra>"

    fig = None
    if chart_type == "area":
        fig = px.area(
            df,
            x="date",
            y=value,
            color=colour_by,
            custom_data=[colour_by],
            title=f"<span class='title'>Daily flows of Russian fossil fuels</span><br><span class='subtitle'>{unit_str}</span>",
            color_discrete_map=palette,
            facet_col=facet,
        )
        for i in range(len(fig["data"])):
            fig["data"][i]["line"]["width"] = 0
        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
        fig.update_xaxes(autorange=True)

    elif chart_type == "line":
        fig = px.line(
            df,
            x="date",
            y=value,
            color=colour_by,
            custom_data=[colour_by],
            title=f"<span class='title'>Daily flows of Russian fossil fuels</span><br><span class='subtitle'>{unit_str}</span>",
            color_discrete_map=palette,
            facet_col=facet,
        )

    elif chart_type == "bar":
        # Get floor month
        frequency = "M"
        group_by = [x for x in ["period", colour_by, facet] if x is not None]
        df["period"] = pd.to_datetime(df.date).dt.to_period(frequency).dt.to_timestamp()
        df = df.groupby(group_by).sum(numeric_only=True).reset_index()
        fig = px.bar(
            df,
            x="period",
            y=value,
            color=colour_by,
            custom_data=[colour_by],
            title=f"<span class='title'>Daily flows of Russian fossil fuels</span><br><span class='subtitle'>{unit_str}</span>",
            color_discrete_map=palette,
            facet_col=facet,
        )

    if not fig:
        return None

    fig.update_traces(
        hovertemplate=hovertemplate,
    )

    fig.update_layout(
        plot_bgcolor="white",
        hovermode="x unified",
        legend_title="",
        legend={"traceorder": "reversed"},
        xaxis_title=None,
        yaxis_title=None,
    )
    return fig
