from flask import Blueprint
from flask_restx import Api, Namespace

from base.utils import DateTimeParseError

routes = Blueprint("routes", __name__)
routes_api = Api(
    routes,
    title="Russia Fossil Tracker API",
    description="Users' guide is available "
    + "<a href='https://docs.google.com/document/d/10_JD9nVtJq4oZtgw4Q7pQ-k_oMjRBFYQiqS5xgUIaTo/edit?usp=sharing' target='_blank'>here</a>",
    default="",
    default_label="",
)


@routes_api.errorhandler(DateTimeParseError)
def handle_datetime_parse_error(error):
    return {"message": f"Check your options: {str(error)}"}, 400


ns_charts = Namespace("Charts", description="For plotting data.", path="/")
ns_flaring = Namespace("Flaring", description="For flaring data.", path="/")
ns_alerts = Namespace("Alerts", description="For shipment alerts.", path="/")
ns_transformed = Namespace(
    "Transformed", description="For pre-transformed data for specific use.", path="/"
)

routes_api.add_namespace(ns_charts)
routes_api.add_namespace(ns_flaring)
routes_api.add_namespace(ns_alerts)
routes_api.add_namespace(ns_transformed)

from .template import *
from .counter import *
from .counter_last import *
from .overland import *
from .price import *
from .entsogflow import *
from .commodity import *
from .alert import *
from .flaring import *
from .comtrade import *

from .kpler_flow import *
from .kpler_trade import *

from .charts import *

from .transformed import *
