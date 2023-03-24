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
    DEFAULT_COLOUR_BY,
    DEFAULT_DESTINATION_COUNTRY,
    DEFAULT_COMMODITY,
    DEFAULT_CHART_TYPE,
)
from . import units
from . import countries
from . import colour_bys, facet_bys, commodities, chart_types
from . import from_type, to_type
from . import refreshing
from .utils import get_from_countries, get_to_countries, to_options

chart_settings = html.Div(
    [
        dbc.Col(
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
                    multi=False,
                    value=DEFAULT_COMMODITY,
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
                            id="btn-download-kpler2",
                            color="secondary",
                        ),
                    ],
                    className="d-grid gap-2 mt-2",
                ),
                dcc.Download(id="download-kpler0"),
                dcc.Download(id="download-kpler2"),
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
                            value=FACET_NONE,
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
                    dcc.Graph(id="kpler-area-chart", className="flex-grow-1"),
                    className="d-flex",
                ),
            ],
            style={"height": "100%", "display": "flex"},
            className="flex-grow-1",
        ),
    ],
    style={"height": "100%"},
)
