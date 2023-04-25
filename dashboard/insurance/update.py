import math

import pandas as pd
import plotly.express as px
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State, dcc
from dash.exceptions import PreventUpdate
from server import app, cache
from utils import palette, opaque_background

from . import COUNTRY_GLOBAL
from . import FACET_NONE, DATE_FROM
from . import units
from . import laundromat_iso2s, pcc_iso2s, eu27_iso2s
from .utils import roll_average_insurance
from .data import get_insurance_full


@app.callback(Output("insurance-rolling-days", "disabled"), Input("insurance-chart-type", "value"))
def update_options(chart_type):
    if not chart_type:
        raise PreventUpdate
    return chart_type in ["bar"]


@app.callback(
    output=Output("insurance-origin-country", "value", allow_duplicate=True),
    inputs=[
        Input("insurance-origin-country-select-laundromat", "n_clicks"),
    ],
    allow_duplicate=True,
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_origin_laundromat(n_clicks):
    return laundromat_iso2s


@app.callback(
    output=Output("insurance-origin-country", "value", allow_duplicate=True),
    inputs=[
        Input("insurance-origin-country-select-russia", "n_clicks"),
    ],
    allow_duplicate=True,
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_origin_laundromat(n_clicks):
    return ["RU"]


@app.callback(
    output=Output("insurance-destination-country", "value", allow_duplicate=True),
    inputs=[
        Input("insurance-destination-country-select-laundromat", "n_clicks"),
    ],
    allow_duplicate=True,
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_origin_laundromat(n_clicks):
    return laundromat_iso2s


@app.callback(
    output=Output(
        "insurance-destination-country",
        "value",
        allow_duplicate=True,
    ),
    inputs=[
        Input("insurance-destination-country-select-pcc", "n_clicks"),
    ],
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_origin_pcc(n_clicks):
    return pcc_iso2s


@app.callback(
    output=Output("insurance-destination-country", "value", allow_duplicate=True),
    inputs=[
        Input("insurance-destination-country-select-eu27", "n_clicks"),
    ],
    allow_duplicate=True,
    suppress_callback_exceptions=True,
    prevent_initial_call=True,
)
def select_destination_eu27(n_clicks):
    return eu27_iso2s


@app.callback(
    output=Output("insurance-area-chart", "figure"),
    inputs=[
        State("insurance-origin-country", "value"),
        State("insurance-destination-country", "value"),
        State("insurance-commodity", "value"),
        Input("insurance-refresh", "n_clicks"),
        Input("colour-by", "value"),
        Input("facet", "value"),
        Input("insurance-rolling-days", "value"),
        # Chart specific
        Input("unit", "value"),
        Input("insurance-chart-type", "value"),
    ],
    suppress_callback_exceptions=True,
)
def update_chart(
    origin_iso2,
    destination_iso2,
    commodity,
    n,
    colour_by,
    facet,
    rolling_days,
    unit_id,
    chart_type,
):
    if facet == FACET_NONE:
        facet = None
    # if n is None:
    #     raise PreventUpdate

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

    unit = units[unit_id]
    value = unit["column"]
    unit_str = unit["label"]
    unit_format = unit["format"]
    unit_scale = unit["scale"]
    df[value] = df[value] * unit_scale
    hovertemplate = f"%{{customdata[0]}}: %{{y:{unit_format}}} {unit_str}<extra></extra>"

    sort_by = []
    if facet is not None:
        facet_col_wrap = math.ceil(math.sqrt(len(df[facet].unique())))
        df[facet] = pd.Categorical(
            df[facet], categories=df.groupby(facet)[value].sum().sort_values(ascending=False).index
        )
        sort_by.append(facet)
    else:
        facet_col_wrap = 1

    if colour_by is not None:

        existing_categories = df[colour_by].unique()
        specific_order = ["G7", "Norway", "Other", "Unknown"]
        if all([any(x in y for x in specific_order) for y in existing_categories]):
            categories = [next(x for x in existing_categories if y in x) for y in specific_order]
        else:
            categories = df.groupby(colour_by)[value].sum().sort_values(ascending=False).index
        df[colour_by] = pd.Categorical(df[colour_by], categories=categories)

        sort_by.append(colour_by)

    df = df.sort_values(sort_by)
    fig = None

    if chart_type == "area_share":
        group_by = [x for x in ["date", facet] if x is not None]
        # Remove commodities without data
        df = df[df[value] > 0]

        df["share"] = df.groupby(group_by)[value].apply(lambda x: x / x.sum())
        fig = px.area(
            df,
            x="date",
            y="share",
            color=colour_by,
            custom_data=[colour_by],
            title=f"<span class='title'><b>Daily flows of Russian fossil fuels</b></span><br><span class='subtitle'>{unit_str} per day</span>",
            color_discrete_map=palette,
            facet_col=facet,
            facet_col_wrap=facet_col_wrap,
            # make it opaque
        )
        fig.for_each_yaxis(lambda x: x.update(tickformat=".0%"))
        fig = opaque_background(fig)

    elif chart_type == "area":
        fig = px.area(
            df,
            x="date",
            y=value,
            color=colour_by,
            custom_data=[colour_by],
            title=f"<span class='title'><b>Daily flows of Russian fossil fuels</b></span><br><span class='subtitle'>{unit_str} per day</span>",
            color_discrete_map=palette,
            facet_col=facet,
            facet_col_wrap=facet_col_wrap,
        )
        fig = opaque_background(fig)

    elif chart_type == "line":
        fig = px.line(
            df,
            x="date",
            y=value,
            color=colour_by,
            custom_data=[colour_by],
            title=f"<span class='title'><b>Daily flows of Russian fossil fuels</b></span><br><span class='subtitle'>{unit_str} per day</span>",
            color_discrete_map=palette,
            facet_col=facet,
            facet_col_wrap=facet_col_wrap,
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
            title=f"<span class='title'><b>Monthly flows of Russian fossil fuels</b></span><br><span class='subtitle'>{unit_str} per month</span>",
            color_discrete_map=palette,
            facet_col=facet,
            facet_col_wrap=facet_col_wrap,
        )

    if not fig:
        return None

    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    # fig.for_each_xaxis(lambda x: x.update(title=None, range=[DATE_FROM, None]))
    fig.for_each_yaxis(lambda x: x.update(title=None))

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
        margin=dict(l=0, r=20),
        title=dict(xref="paper", xanchor="left", x=0),
    )

    # def update_opacity(figure, opacity):
    #     for trace in range(len(figure['data'])):
    #         # print(figure['data'][trace]['fillcolor'],'-> ',end='')
    #         rgba_split = figure['data'][trace]['fillcolor'].split(',')
    #         figure['data'][trace]['fillcolor'] = ','.join(rgba_split[:-1] + [' {})'.format(opacity)])
    #         # print(figure['data'][trace]['fillcolor'])
    #     return figure
    #
    # # fig = update_opacity(fig, 1)
    return fig
