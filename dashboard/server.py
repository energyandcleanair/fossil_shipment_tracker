import os
from decouple import config
import dash
import dash_auth

import diskcache
import dash_bootstrap_components as dbc
from dash import DiskcacheManager, CeleryManager, Input, Output, html
from flask_caching import Cache

launch_uid = "RFT"

# Create a Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)

app.title = "Russia Fossil Tracker"

# Add basic authentication
VALID_USERNAME_PASSWORD_PAIRS = {config("USERNAME"): config("PASSWORD")}

auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)


# Expose the server variable
server = app.server
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": config("REDISURL", "redis://localhost:6379"),
    "CACHE_DEFAULT_TIMEOUT": 60 * 60 * 24,
}
cache = Cache()
cache.init_app(app.server, config=CACHE_CONFIG)
