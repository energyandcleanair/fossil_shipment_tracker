import pandas as pd
import plotly.express as px
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State, dcc
from dash.exceptions import PreventUpdate
from server import app, cache
from utils import palette

from . import COUNTRY_GLOBAL
from . import FACET_NONE
from . import units
from . import laundromat_iso2s, pcc_iso2s, eu27_iso2s
from .utils import roll_average_kpler
from .data import get_kpler1


@app.callback(Output("kpler-rolling-days", "disabled"), Input("kpler-chart-type", "value"))
def update_options(chart_type):
    if not chart_type:
        raise PreventUpdate
    return chart_type in ["bar"]


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
    output=Output("kpler-destination-country", "value", allow_duplicate=True),
    inputs=[
        Input("kpler-destination-country-select-eu27", "n_clicks"),
    ],
    allow_duplicate=True,
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_destination_eu27(n_clicks):
    return eu27_iso2s


@app.callback(
    output=Output("kpler-area-chart", "figure"),
    inputs=[
        Input("kpler0", "data"),
        Input("colour-by", "value"),
        Input("facet", "value"),
        Input("kpler-rolling-days", "value"),
        # Chart specific
        Input("unit", "value"),
        Input("kpler-chart-type", "value"),
    ],
    suppress_callback_exceptions=True,
)
def update_chart(kpler0, colour_by, facet, rolling_days, unit_id, chart_type):
    if facet == FACET_NONE:
        facet = None
    if kpler0 is None:
        raise PreventUpdate

    if chart_type == "bar":
        df = get_kpler1(kpler0, colour_by, facet, 1)
    else:
        df = get_kpler1(kpler0, colour_by, facet, rolling_days)
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
            title=f"<span class='title'>Daily flows of Russian fossil fuels</span><br><span class='subtitle'>{unit_str} per day</span>",
            color_discrete_map=palette,
            facet_col=facet,
        )
        for i in range(len(fig["data"])):
            fig["data"][i]["line"]["width"] = 0

    elif chart_type == "line":
        fig = px.line(
            df,
            x="date",
            y=value,
            color=colour_by,
            custom_data=[colour_by],
            title=f"<span class='title'>Daily flows of Russian fossil fuels</span><br><span class='subtitle'>{unit_str} per day</span>",
            color_discrete_map=palette,
            facet_col=facet,
        )

    elif chart_type == "bar":
        # Get floor month
        frequency = "M"
        group_by = [x for x in ["period", colour_by, facet] if x is not None]
        df["period"] = pd.to_datetime(df.date).dt.to_period(frequency).dt.to_timestamp()
        df = df.groupby(group_by)[value].sum().reset_index()
        fig = px.bar(
            df,
            x="period",
            y=value,
            color=colour_by,
            custom_data=[colour_by],
            title=f"<span class='title'>Monthly flows of Russian fossil fuels</span><br><span class='subtitle'>{unit_str} per month</span>",
            color_discrete_map=palette,
            facet_col=facet,
        )

    if not fig:
        return None

    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.for_each_xaxis(lambda x: x.update(title=None))
    fig.update_xaxes(autorange=True)

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
