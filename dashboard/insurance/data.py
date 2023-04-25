import pandas as pd
import dash
import requests
from decouple import config
from dash import Input, Output, html, State
from dash.exceptions import PreventUpdate

from logger import logger
from server import app, cache
from . import COUNTRY_GLOBAL, DATE_FROM
from . import FACET_NONE
from . import COMMODITY_ALL
from .utils import to_list, roll_average_insurance, add_insurer_owner_region

"""
We create several level of kpler data.
Not all parameter changes require a new data query to the API, or roll-averaging.
"""


# perform expensive computations in this "global store"
# these computations are cached in a globally available
# redis memory store which is available across processes
# and for all time.
@cache.memoize()
def get_insurance0(origin_iso2, destination_iso2, commodity):
    # simulate expensive query
    print("=== loading insurance ===")
    # columns = [
    #     "origin_name",
    #     "destination_name",
    #     "destination_region",
    #     "date",
    #     "product",
    #     "product_group",
    #     "product_family",
    #     "commodity_equivalent_name",
    #     "value_tonne",
    #     "value_eur",
    #     "value_usd",
    # ]
    aggregate_by = [
        "ship_owner_country",
        "ship_insurer_country",
        "departure_date",
        "commodity_group",
    ]
    params = {
        "commodity_origin_iso2": ",".join(to_list(origin_iso2)),
        "commodity_destination_iso2_not": ",".join(to_list(origin_iso2)),
        "aggregate_by": ",".join(aggregate_by),
        "use_eu": True,
        "status": ",".join(["ongoing", "completed"]),
        "date_from": "2021-01-01",
        "commodity_grouping": "split_gas_oil",
    }

    if COUNTRY_GLOBAL not in to_list(destination_iso2):
        params["commodity_destination_iso2"] = ",".join(to_list(destination_iso2))

    if COMMODITY_ALL not in to_list(commodity):
        params["commodity"] = ",".join(to_list(commodity))

    url = "https://api.russiafossiltracker.com/v0/voyage"
    r = requests.get(url, params=params)
    data = r.json()
    print("=== done ===")
    return data.get("data")


# @dash.callback(
#     output=Output("kpler0", "data"),
#     inputs=[
#         State("kpler-origin-country", "value"),
#         State("kpler-origin-type", "value"),
#         State("kpler-destination-country", "value"),
#         State("kpler-destination-type", "value"),
#         State("kpler-commodity", "value"),
#         Input("kpler-refresh", "n_clicks"),
#     ],
# )
# def load_kpler0(origin_iso2, origin_type, destination_iso2, destination_type, commodity, n):
#     if n is not None:
#         return get_kpler0(origin_iso2, origin_type, destination_iso2, destination_type, commodity)
#     else:
#         raise PreventUpdate


# @cache.memoize()
def get_insurance_full(
    origin_iso2,
    destination_iso2,
    commodity,
    colour_by,
    facet,
    rolling_days,
):

    insurance0 = get_insurance0(origin_iso2, destination_iso2, commodity)
    df = pd.DataFrame(insurance0)

    df = add_insurer_owner_region(df)
    df = df.rename(columns={"departure_date": "date"})

    aggregate_by = list(set(["date"] + [colour_by] + [facet]))
    aggregate_by = [x for x in aggregate_by if x is not None]
    value_cols = [x for x in df.columns if x.startswith("value_")]
    df = df.groupby(aggregate_by)[value_cols].sum(numeric_only=True).reset_index()

    # Group largest colours together
    value_cols = [x for x in df.columns if x.startswith("value_")]
    largest = df.groupby(colour_by)[value_cols].sum().nlargest(9, columns=value_cols[0]).index
    df.loc[~df[colour_by].isin(largest), colour_by] = "Other"
    df = df.groupby(aggregate_by)[value_cols].sum(numeric_only=True).reset_index()

    # Remove all first rows of df until the first date with a non-zero value
    value_cols = [x for x in df.columns if x.startswith("value_")]
    min_date = df.loc[(df[value_cols] > 0).apply(any, axis=1)]["date"].min()
    df = df[df["date"] >= min_date]
    df = roll_average_insurance(df, rolling_days)

    df = df[pd.to_datetime(df.date) >= pd.to_datetime(DATE_FROM)]
    return df


#
# @app.callback(
#     output=Output("kpler1", "data"),
#     inputs=[
#         Input("kpler0", "data"),
#         Input("colour-by", "value"),
#         Input("facet", "value"),
#         Input("kpler-rolling-days", "value"),
#     ],
# )
# def load_kpler1(kpler0, colour_by, facet, rolling_days):
#     if facet == FACET_NONE:
#         facet = None
#     if kpler0 is None:
#         raise PreventUpdate
#     logger.info("=== kpler1: reading json ===")
#     df = get_kpler1(kpler0, colour_by, facet, rolling_days)
#     result = df.to_dict(orient="split")
#     return result

# @app.callback(
#     output=Output("kpler_full", "data"),
#     inputs=[
#         State("kpler-origin-country", "value"),
#         State("kpler-origin-type", "value"),
#         State("kpler-destination-country", "value"),
#         State("kpler-destination-type", "value"),
#         State("kpler-commodity", "value"),
#         Input("kpler-refresh", "n_clicks"),
#         Input("colour-by", "value"),
#         Input("facet", "value"),
#         Input("kpler-rolling-days", "value"),
#     ],
# )
# def load_kpler_full(origin_iso2, origin_type, destination_iso2, destination_type, commodity, n,
#                 colour_by, facet, rolling_days):
#     if facet == FACET_NONE:
#         facet = None
#     if n is None:
#         raise PreventUpdate
#
#     df = get_kpler_full(origin_iso2, origin_type, destination_iso2,
#                         destination_type, commodity,
#                         colour_by, facet, rolling_days)
#     result = df.to_dict(orient="split")
#     return result
