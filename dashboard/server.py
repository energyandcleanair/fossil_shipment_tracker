import dash
import diskcache
import dash_bootstrap_components as dbc
from dash import DiskcacheManager, CeleryManager, Input, Output, html

launch_uid = "RFT"
cache = diskcache.Cache("./cache")
background_callback_manager = DiskcacheManager(cache, cache_by=[lambda: launch_uid], expire=6000)

# Create a Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    background_callback_manager=background_callback_manager,
    suppress_callback_exceptions=True,
)

# Expose the server variable
server = app.server
