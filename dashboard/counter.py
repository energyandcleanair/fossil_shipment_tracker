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
from utils import roll_average_counter, palette

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
                        # dbc.InputGroupText("Rolling days:"),
                        dbc.Label("Rolling days:", size="sm"),
                        dbc.Input(id="rolling-days", type="number", value=14, size="sm"),
                    ],
                    className="md-2",
                ),
                dbc.Col(
                    [
                        dbc.Label("Colour by:", size="sm"),
                        dbc.Select(
                            id="colour-by",
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
                            id="unit",
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
                            id="facet",
                            options=[
                                {"label": "-", "value": FACET_NONE},
                                {"label": "Region", "value": "destination_region"},
                                {"label": "Country", "value": "destination_country"},
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
                dbc.Col(
                    [
                        dbc.Label("Country:", size="sm"),
                        dcc.Dropdown(
                            id="destination-country",
                            multi=True,
                            value=COUNTRY_GLOBAL,
                            style={"min-width": "150px"},
                            maxHeight=300,
                        ),
                    ],
                    className="md-3",
                ),
            ],
        ),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="area-chart", className="flex-grow-1"), className="d-flex"),
            ],
            style={"height": "100%", "display": "flex"},
            className="flex-grow-1",
        ),
    ],
    style={"height": "100%"},
)


@app.callback(
    output=Output("counter-rolled", "data"),
    inputs=[
        Input("counter", "data"),
        Input("rolling-days", "value"),
    ],
)
def counter_rolled(json_data, rolling_days):
    if json_data is None:
        raise PreventUpdate
    df = pd.read_json(json_data, orient="split")
    df = roll_average_counter(df, rolling_days).reset_index()
    return df.to_json(orient="split")


@app.callback(
    Output("destination-country", "options"),
    [Input("counter", "data")],
    State("destination-country", "value"),
)
def update_destination_country(counter, value):
    if not counter:
        raise PreventUpdate
    counter = pd.read_json(counter, orient="split")
    options = [COUNTRY_GLOBAL] + list(counter["destination_country"].unique())

    return [{"label": o, "value": o} for o in options if o is not None]
    # options = [
    #     {"label": "New York City", "value": "NYC"},
    #     {"label": "Montreal", "value": "MTL"},
    #     {"label": "San Francisco", "value": "SF"},
    # ]
    # # Make sure that the set values are in the option list, else they will disappear
    # # from the shown select list, but still part of the `value`.
    # return [o for o in options if search_value in o["label"] or o["value"] in (value or [])]


@app.callback(
    output=Output("area-chart", "figure"),
    inputs=[
        Input("counter-rolled", "data"),
        Input("colour-by", "value"),
        Input("unit", "value"),
        Input("destination-country", "value"),
        Input("facet", "value"),
    ],
    suppress_callback_exceptions=True,
)
def update_counter(json_data, colour_by, unit_id, destination_country, facet):
    if facet == FACET_NONE:
        facet = None
    if json_data is None:
        raise PreventUpdate
    df = pd.read_json(json_data, orient="split")
    if destination_country and COUNTRY_GLOBAL not in destination_country:
        df = df[df["destination_country"].isin(destination_country)]
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

    # Remove all first rows of df until the first date with a non-zero value
    min_date = df.loc[df[value] > 0]["date"].min()
    df = df[df["date"] >= min_date]

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
