import pandas as pd
from dash import dcc
from dash import html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
from dash import DiskcacheManager, CeleryManager, Input, Output, html, State

from . import COUNTRY_GLOBAL
from . import FACET_NONE
from . import (
    DEFAULT_ROLLING_DAYS,
    DEFAULT_TOP_N,
    DEFAULT_COLOUR_BY,
    DEFAULT_DESTINATION_COUNTRY,
    DEFAULT_COMMODITIES,
    DEFAULT_CHART_TYPE,
    DEFAULT_FACET,
)
from . import units
from . import countries
from . import colour_bys, facet_bys, commodities, chart_types
from . import from_type, to_type
from . import refreshing
from .utils import get_from_countries, get_to_countries, to_options

chart_settings = html.Div(
    [
        dbc.Label("From:", size="sm"),
        dcc.Dropdown(
            id="kpler-origin-country",
            options=get_from_countries(),
            multi=True,
            value="RU",
            style={"min-width": "150px"},
        ),
        html.Div(
            [
                dbc.Button(
                    "Russia",
                    id="kpler-origin-country-select-russia",
                    color="primary",
                    className="btn-preset",
                ),
                dbc.Button(
                    "Laudromat",
                    id="kpler-origin-country-select-laundromat",
                    color="primary",
                    className="btn-preset",
                ),
            ],
            className="btn-preset-group",
        ),
        dbc.Label("To:", size="sm"),
        dcc.Dropdown(
            id="kpler-destination-country",
            options=get_to_countries(),
            multi=True,
            value=DEFAULT_DESTINATION_COUNTRY,
            style={"min-width": "150px"},
            maxHeight=150,
        ),
        html.Div(
            [
                dbc.Button(
                    "Laudromat",
                    id="kpler-destination-country-select-laundromat",
                    color="primary",
                    className="btn-preset",
                ),
                dbc.Button(
                    "PCC",
                    id="kpler-destination-country-select-pcc",
                    color="primary",
                    className="btn-preset",
                ),
                dbc.Button(
                    "EU-27",
                    id="kpler-destination-country-select-eu27",
                    color="primary",
                    className="btn-preset",
                ),
            ],
            className="btn-preset-group",
        ),
        dbc.Label("Origin by:", size="sm"),
        dcc.Dropdown(
            id="kpler-origin-type",
            options=from_type,
            multi=False,
            value="country",
            style={"min-width": "150px"},
        ),
        dbc.Label("Destination by:", size="sm"),
        dcc.Dropdown(
            id="kpler-destination-type",
            options=to_type,
            multi=False,
            value="country",
            style={"min-width": "150px"},
        ),
        dbc.Label("Commodities:", size="sm"),
        dcc.Dropdown(
            id="kpler-commodity",
            options=commodities,
            multi=True,
            value=DEFAULT_COMMODITIES,
            style={"min-width": "150px"},
        ),
        html.Div(
            [
                dbc.Button(
                    " Refresh data",
                    id="kpler-refresh",
                    color="primary",
                )
            ],
            className="d-grid gap-2 mt-2",
        ),
        # dbc.InputGroupText("Rolling days:"),
        html.Div(
            [
                dbc.Button(
                    "Download raw data",
                    id="btn-download-kpler0",
                    color="secondary",
                ),
                dbc.Button(
                    "Download processed data",
                    id="btn-download-kpler1",
                    color="secondary",
                ),
            ],
            className="d-grid gap-2 mt-2",
        ),
        dcc.Download(id="download-kpler0"),
        dcc.Download(id="download-kpler1"),
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
                    [
                        dbc.Label("Chart type:", size="sm"),
                        dbc.Select(
                            id="kpler-chart-type",
                            options=chart_types,
                            value=DEFAULT_CHART_TYPE,
                            size="sm",
                        ),
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Label("Rolling days:", size="sm"),
                        dbc.Input(
                            id="kpler-rolling-days",
                            type="number",
                            value=DEFAULT_ROLLING_DAYS,
                            size="sm",
                        ),
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Label("Colour by:", size="sm"),
                        dbc.Select(
                            id="colour-by",
                            options=colour_bys,
                            value=DEFAULT_COLOUR_BY,
                            size="sm",
                        ),
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Label("Top N:", size="sm"),
                        dbc.Input(
                            id="kpler-top-n",
                            type="number",
                            value=DEFAULT_TOP_N,
                            size="sm",
                        ),
                    ]
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
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Label("Facet by:", size="sm"),
                        dbc.Select(
                            id="facet",
                            options=facet_bys,
                            value=DEFAULT_FACET,
                            size="sm",
                        ),
                    ]
                ),
            ],
            className="mb-1",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Loading(
                        id="loading-kpler",
                        type="default",
                        parent_className="loading d-flex flex-grow-1",
                        # fullscreen=True,
                        children=[dcc.Graph(id="kpler-area-chart", className="flex-grow-1")],
                    ),
                    className="d-flex",
                ),
            ],
            style={"height": "100%", "display": "flex"},
            className="flex-grow-1",
        ),
    ],
    style={"height": "100%"},
)
