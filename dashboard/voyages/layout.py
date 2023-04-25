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
        dbc.Label("Departure:", size="sm"),
        dcc.Dropdown(
            id="voyages-departure-country",
            multi=False,
            value="Russia",
            style={"min-width": "150px"},
        ),
        dbc.Label("Destination:", size="sm"),
        dcc.Dropdown(
            id="voyages-destination-country",
            multi=True,
            value=COUNTRY_GLOBAL,
            style={"min-width": "150px"},
        ),
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
        dbc.Label("Rolling days:"),
        dbc.Input(id="rolling-days", type="number", value=14),
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
            id="voyages-unit",
            options=[{"label": v["label"], "value": k} for k, v in units.items()],
            value=list(units.keys())[0],
            size="sm",
        ),
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
    ]
)


layout = html.Div(
    [
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
