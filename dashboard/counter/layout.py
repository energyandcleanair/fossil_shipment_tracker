from dash import dcc
from dash import html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State

from . import COUNTRY_GLOBAL
from . import FACET_NONE
from . import units

chart_settings = html.Div(
    [
        dbc.Col(
            [
                # dbc.InputGroupText("Rolling days:"),
                dbc.Label("Rolling days:", size="sm"),
                dbc.Input(id="rolling-days", type="number", value=14, size="sm"),
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
                dbc.Label("Unit:", size="sm"),
                dbc.Select(
                    id="unit",
                    options=[{"label": v["label"], "value": k} for k, v in units.items()],
                    value=list(units.keys())[0],
                    size="sm",
                ),
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
                dbc.Label("Country:", size="sm"),
                dcc.Dropdown(
                    id="destination-country",
                    multi=True,
                    value=COUNTRY_GLOBAL,
                    style={"min-width": "150px"},
                ),
            ]
        )
    ]
)

layout = html.Div(
    [
        # A row with:
        # - an integer input for rolling_days,
        # - a dropdown for aggregate_by
        # - a multiselect with all the regions
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(id="counter-area-chart", className="flex-grow-1"), className="d-flex"
                ),
            ],
            style={"height": "100%", "display": "flex"},
            className="flex-grow-1",
        ),
    ],
    style={"height": "100%"},
)
