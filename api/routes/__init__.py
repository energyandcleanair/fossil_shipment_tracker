from flask import Blueprint
from flask_restx import Api

routes = Blueprint('routes', __name__)
routes_api = Api(routes)

from .ship import *
from .port import *
from .voyage import *
from .position import *

