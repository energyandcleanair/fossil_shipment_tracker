import pandas as pd
import plotly.express as px
import dash
import diskcache
import requests
from dash import dcc
from dash import html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State
from dash.exceptions import PreventUpdate
from server import app
from utils import palette, roll_average_voyage

COUNTRY_GLOBAL = "Global"
FACET_NONE = "None"

units = {
    "thousand_tonne": {
        "label": "Thousand tonne",
        "column": "value_tonne",
        "format": ",.0f",
        "scale": 1e-3,
    },
    "million_eur": {
        "label": "Million EUR",
        "column": "value_eur",
        "format": ",.0f",
        "scale": 1e-6,
    },
    "million_usd": {
        "label": "Million USD",
        "column": "value_eur",
        "format": ",.0f",
        "scale": 1e-6,
    },
}

layout = html.Div(
    [
        # A row with:
        # - an integer input for rolling_days,
        # - a dropdown for aggregate_by
        # - a multiselect with all the regions
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Label("Departure:", size="sm"),
                        dcc.Dropdown(
                            id="voyages-departure-country",
                            multi=False,
                            value="Russia",
                            style={"min-width": "150px"},
                        ),
                    ],
                    className="md-3",
                ),
                dbc.Col(
                    [
                        dbc.Label("Destination:", size="sm"),
                        dcc.Dropdown(
                            id="voyages-destination-country",
                            multi=True,
                            value=COUNTRY_GLOBAL,
                            style={"min-width": "150px"},
                        ),
                    ],
                    className="md-3",
                ),
                dbc.Col(
                    [
                        dbc.Label("Status:", size="sm"),
                        dcc.Dropdown(
                            id="voyages-status",
                            multi=True,
                            options=[
                                {"label": "Completed", "value": "completed"},
                                {"label": "Ongoing", "value": "ongoing"},
                            ],
                            value=["completed", "ongoing"],
                        ),
                    ],
                    className="md-2",
                ),
                dbc.Col(
                    [
                        dbc.Label("Rolling days:"),
                        dbc.Input(id="voyages-rolling-days", type="number", value=14),
                    ],
                    className="md-2",
                ),
                dbc.Col(
                    [
                        dbc.Label("Colour by:", size="sm"),
                        dbc.Select(
                            id="voyages-colour-by",
                            options=[
                                {"label": "Region", "value": "destination_region"},
                                {
                                    "label": "Commodity group",
                                    "value": "commodity_group_name",
                                },
                            ],
                            value="destination_region",
                            size="sm",
                        ),
                    ],
                    className="md-3",
                ),
                dbc.Col(
                    [
                        dbc.Label("Unit:", size="sm"),
                        dbc.Select(
                            id="voyages-unit",
                            options=[{"label": v["label"], "value": k} for k, v in units.items()],
                            value=list(units.keys())[0],
                            size="sm",
                        ),
                    ],
                    className="md-2",
                ),
                dbc.Col(
                    [
                        dbc.Label("Facet by:", size="sm"),
                        dbc.Select(
                            id="voyages-facet",
                            options=[
                                {"label": "-", "value": FACET_NONE},
                                {
                                    "label": "Region",
                                    "value": "commodity_destination_region",
                                },
                                {
                                    "label": "Country",
                                    "value": "commodity_destination_country",
                                },
                                {
                                    "label": "Commodity group",
                                    "value": "commodity_group_name",
                                },
                            ],
                            value=FACET_NONE,
                            size="sm",
                        ),
                    ],
                    className="md-3",
                ),
            ],
        ),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="voyages-chart", className="flex-grow-1"), className="d-flex"),
            ],
            style={"height": "100%", "display": "flex"},
            className="flex-grow-1",
        ),
    ],
    style={"height": "100%"},
)


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
        Input("voyages-colour-by", "value"),
        Input("voyages-unit", "value"),
        Input("voyages-status", "value"),
        Input("voyages-rolling-days", "value"),
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
