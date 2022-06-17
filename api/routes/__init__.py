from flask import Blueprint
from flask_restx import Api

routes = Blueprint('routes', __name__)
routes_api = Api(routes)

from .ship import *
from .port import *
from .voyage import *
from .position import *
from .berth import *
from .portcall import *
from .departure import *
from .counter import *
from .counter_last import *
from .overland import *
from .price import *
from .entsogflow import *
from .commodity import *