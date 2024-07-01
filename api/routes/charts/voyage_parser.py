from flask_restx import reqparse, inputs

from base import (
    PRICING_DEFAULT,
    COMMODITY_GROUPING_DEFAULT,
    COMMODITY_GROUPING_CHOICES,
    COMMODITY_GROUPING_HELP,
)

import datetime as dt

voyage_parser = reqparse.RequestParser()

# Query content
voyage_parser.add_argument(
    "bypass_maintenance",
    help="bypass maintenance when in maintenance",
    default=False,
    required=False,
    type=inputs.boolean,
)

voyage_parser.add_argument(
    "id",
    help="id(s) of voyage. Default: returns all of them",
    default=None,
    action="split",
    required=False,
)
voyage_parser.add_argument(
    "commodity",
    help="commodity(ies) of interest. Default: returns all of them",
    default=None,
    action="split",
    required=False,
)
voyage_parser.add_argument(
    "commodity_group",
    help="commodity group(s) of interest. e.g. oil,gas,coal Default: returns all of them",
    default=None,
    action="split",
    required=False,
)
voyage_parser.add_argument(
    "status",
    help="status of shipments. Could be any or several of completed, ongoing, undetected_arrival. Default: returns all of them",
    default=None,
    action="split",
    required=False,
)
voyage_parser.add_argument(
    "is_sts",
    help="denotes whether a shipment has had sts. Can be either True or False. Default: returns both",
    default=None,
    required=False,
)

voyage_parser.add_argument(
    "date_from",
    help="start date for departure or arrival (format 2020-01-15)",
    default=None,
    required=False,
)
voyage_parser.add_argument(
    "departure_date_from",
    help="start date for departure (format 2020-01-15)",
    default=None,
    required=False,
)
voyage_parser.add_argument(
    "arrival_date_from",
    help="start date for arrival (format 2020-01-15)",
    default=None,
    required=False,
)

voyage_parser.add_argument(
    "date_to",
    type=str,
    help="end date for departure or arrival (format 2020-01-15 or -7 for seven days before today)",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "departure_date_to",
    type=str,
    help="end date for departure (format 2020-01-15 or -7 for seven days before today)",
    required=False,
    default=dt.datetime.today().strftime("%Y-%m-%d"),
)
voyage_parser.add_argument(
    "arrival_date_to",
    type=str,
    help="end date for arrival (format 2020-01-15 or -7 for seven days before today)",
    required=False,
    default=None,
)

voyage_parser.add_argument(
    "year",
    help="year(s) of departure or arrival e.g. 2021,2022",
    type=int,
    default=None,
    required=False,
    action="split",
)
voyage_parser.add_argument(
    "departure_year",
    help="year(s) of departure e.g. 2021,2022",
    type=int,
    default=None,
    required=False,
    action="split",
)
voyage_parser.add_argument(
    "arrival_year",
    help="year(s) of arrival e.g. 2021,2022",
    type=int,
    default=None,
    required=False,
    action="split",
)

voyage_parser.add_argument(
    "pricing_scenario",
    help="Pricing scenario (standard or pricecap)",
    action="split",
    default=[PRICING_DEFAULT],
    required=False,
)

voyage_parser.add_argument(
    "ship_imo",
    action="split",
    help="IMO identifier(s) of the ship(s)",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "commodity_origin_iso2",
    action="split",
    help="iso2(s) of origin of commodity.",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "departure_iso2",
    action="split",
    help="iso2(s) of departure (only RU should be available)",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "departure_port_id",
    action="split",
    help="ids (CREA database id) of departure ports to consider",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "departure_berth_id",
    action="split",
    help="ids (CREA database id) of departure berth to consider",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "departure_port_unlocode",
    action="split",
    help="unlocode of departure ports to consider",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "departure_port_area",
    action="split",
    help="area of departure ports to consider e.g. Baltic,Arctic,Pacific,Black Sea,Caspian Sea",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "destination_iso2",
    action="split",
    help="iso2(s) of destination",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "destination_iso2_not",
    action="split",
    help="countries(s) of destination to exclude e.g. RU",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "destination_region",
    action="split",
    help="region(s) of destination e.g. EU,Turkey",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "commodity_destination_iso2",
    action="split",
    help="ISO2(s) of commodity destination country",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "commodity_destination_iso2_not",
    action="split",
    help="ISO2(s) of commodity destination country TO EXCLUDE",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "commodity_destination_region",
    action="split",
    help="region(s) of commodity destination e.g. EU28,Turkey",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "commodity_grouping",
    type=str,
    help=COMMODITY_GROUPING_HELP,
    default=COMMODITY_GROUPING_DEFAULT,
    choices=COMMODITY_GROUPING_CHOICES,
)
voyage_parser.add_argument(
    "currency",
    action="split",
    help="currency(ies) of returned results e.g. EUR,USD,GBP",
    required=False,
    default=["EUR", "USD"],
)
voyage_parser.add_argument(
    "routed_trajectory",
    help="whether or not to use (re)routed trajectories for those that go over land (only applicable if format=geojson)",
    required=False,
    type=inputs.boolean,
    default=True,
)

voyage_parser.add_argument(
    "ship_owner_iso2",
    action="split",
    help="iso2(s) of ship owner",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "ship_owner_region",
    action="split",
    help="region(s) of ship owner e.g. EU,Turkey",
    required=False,
    default=None,
)

voyage_parser.add_argument(
    "ship_manager_iso2",
    action="split",
    help="iso2(s) of ship manager",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "ship_manager_region",
    action="split",
    help="region(s) of ship manager e.g. EU,Turkey",
    required=False,
    default=None,
)

voyage_parser.add_argument(
    "ship_insurer_iso2",
    action="split",
    help="iso2(s) of ship insurer",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "ship_insurer_region",
    action="split",
    help="region(s) of ship insurer e.g. EU,Turkey",
    required=False,
    default=None,
)

# Query processing
voyage_parser.add_argument(
    "aggregate_by",
    type=str,
    action="split",
    default=None,
    help="which variables to aggregate by. Could be any of commodity, status, departure_date, arrival_date, departure_port, departure_country,"
    "destination_port, destination_country, destination_region",
)
voyage_parser.add_argument(
    "rolling_days",
    type=int,
    help="rolling average window (in days). Default: no rolling averaging",
    required=False,
    default=None,
)

# Query format
voyage_parser.add_argument(
    "format",
    type=str,
    help="format of returned results (json, geojson or csv)",
    required=False,
    default="json",
)
voyage_parser.add_argument(
    "nest_in_data",
    help="Whether to nest the geojson content in a data key.",
    type=inputs.boolean,
    default=True,
)
voyage_parser.add_argument(
    "download",
    help="Whether to return results as a file or not.",
    type=inputs.boolean,
    default=False,
)
voyage_parser.add_argument(
    "pivot_by",
    type=str,
    help="pivoting value_eur (or any other specified by pivot_value) by e.g. commodity_group.",
    required=False,
    action="split",
    default=None,
)
voyage_parser.add_argument(
    "pivot_value",
    type=str,
    help="pivoted value. Default: value_eur.",
    required=False,
    default="value_eur",
)

# Misc
voyage_parser.add_argument(
    "limit",
    type=int,
    help="how many result records do you want (default: keeps all)",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "limit_by",
    action="split",
    help="in which group do you want to limit to n records",
    required=False,
    default=None,
)
voyage_parser.add_argument(
    "sort_by",
    type=str,
    help="sorting results e.g. asc(commodity),desc(value_eur)",
    required=False,
    action="split",
    default=None,
)
voyage_parser.add_argument(
    "select",
    type=str,
    help="selecting specific columns only, with the possibility to rename e.g. new_name1(var1),var2,var3",
    required=False,
    action="split",
    default=None,
)
voyage_parser.add_argument(
    "select_set",
    type=str,
    help="Pre-determined set of columns to return. Default: all columns. Other options are: light",
    required=False,
    default=None,
)

voyage_parser.add_argument(
    "map_unconfirmed_region_eu_to_unknown",
    type=inputs.boolean,
    help="Maps destination region to unknown if the destination of the EU is not likely.",
    required=False,
    default=False,
)
