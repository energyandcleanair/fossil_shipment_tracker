import pandas as pd
import plotly.express as px
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State
from dash.exceptions import PreventUpdate
from server import app, background_callback_manager, cache
from utils import palette

from . import COUNTRY_GLOBAL
from . import FACET_NONE
from . import units
from .utils import roll_average_voyage


@app.callback(
    Output("voyages-departure-country", "options"),
    [Input("voyages", "data")],
    State("voyages-departure-country", "value"),
)
def update_departure_country(voyages, value):
    if not voyages:
        raise PreventUpdate
    voyages = pd.read_json(voyages, orient="split")
    options = [COUNTRY_GLOBAL] + list(voyages["departure_country"].unique())
    return [{"label": o, "value": o} for o in options if o is not None]


@app.callback(
    Output("voyages-destination-country", "options"),
    [Input("voyages", "data")],
    State("voyages-destination-country", "value"),
)
def update_destination_country(voyages, value):
    if not voyages:
        raise PreventUpdate
    voyages = pd.read_json(voyages, orient="split")
    options = [COUNTRY_GLOBAL] + list(voyages["destination_country"].unique())

    return [{"label": o, "value": o} for o in options if o is not None]


@app.callback(
    output=Output("voyages-chart", "figure"),
    inputs=[
        Input("voyages", "data"),
        Input("colour-by", "value"),
        Input("voyages-unit", "value"),
        Input("voyages-status", "value"),
        Input("rolling-days", "value"),
        Input("voyages-departure-country", "value"),
        Input("voyages-destination-country", "value"),
        Input("voyages-facet", "value"),
    ],
    suppress_callback_exceptions=True,
)
def update_voyages(
    json_data,
    colour_by,
    unit_id,
    status,
    rolling_days,
    departure_country,
    destination_country,
    facet,
):
    if facet == FACET_NONE:
        facet = None
    if json_data is None:
        raise PreventUpdate
    df = pd.read_json(json_data, orient="split")
    if departure_country and COUNTRY_GLOBAL not in departure_country:
        df = df[df["commodity_origin_country"] == departure_country]
    if destination_country and COUNTRY_GLOBAL not in destination_country:
        df = df[df["commodity_destination_country"].isin(destination_country)]

    df = df[df.commodity_destination_country != df.commodity_origin_country]
    df = df[df.status.isin(status)]
    df["date"] = pd.to_datetime(df["departure_date_utc"]).dt.date
    aggregate_by = list(set(["date"] + [colour_by] + [facet]))
    aggregate_by = [x for x in aggregate_by if x is not None]
    unit = units[unit_id]
    value = unit["column"]
    unit_str = unit["label"]
    unit_format = unit["format"]
    unit_scale = unit["scale"]
    df[value] = df[value] * unit_scale

    hovertemplate = f"%{{customdata[0]}}: %{{y:{unit_format}}} {unit_str}<extra></extra>"
    df = df.groupby(aggregate_by)[value].sum().reset_index()

    df = roll_average_voyage(df, rolling_days, value)

    # Remove all first rows of df until the first date with a non-zero value
    min_date = df.loc[df[value] > 0]["date"].min()
    df = df[df["date"] >= min_date]

    fig = px.area(
        df,
        x="date",
        y=value,
        color=colour_by,
        custom_data=[colour_by],
        title=f"<span class='title'>Daily shipments of Russian fossil fuels</span><br><span class='subtitle'>{unit_str}</span>",
        color_discrete_map=palette,
        facet_col=facet,
    )
    fig.update_traces(
        hovertemplate=hovertemplate,
    )
    for i in range(len(fig["data"])):
        fig["data"][i]["line"]["width"] = 0
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_xaxes(autorange=True)
    # Hover template: destination_region: value unit
    fig.update_layout(
        plot_bgcolor="white",
        hovermode="x unified",
        legend_title="",
        legend={"traceorder": "reversed"},
        xaxis_title=None,
        yaxis_title=None,
    )

    return fig
