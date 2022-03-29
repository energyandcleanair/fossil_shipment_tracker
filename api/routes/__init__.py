from flask import Blueprint
from flask_restx import Api

routes = Blueprint('routes', __name__)
routes_api = Api(routes)

from .ship import *
from .port import *
from .voyage import *
from .position import *


# Trying to have more explicit error 400
@routes_api.errorhandler(Exception)
def handle_custom_exception(error):
    '''Return a custom message and 400 status code'''
    return {'message2': str(error)}, error.code