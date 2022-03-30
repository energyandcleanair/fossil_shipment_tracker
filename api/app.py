import os
from base.db import session
from werkzeug.exceptions import HTTPException
from flask import Flask, request
from flask import jsonify
from flask_cors import CORS
from routes import routes

try:
    import googleclouddebugger
    googleclouddebugger.enable(
        breakpoint_enable_canary=True
    )
except ImportError:
    pass

app = Flask(__name__)
app.config.SWAGGER_UI_DOC_EXPANSION = 'list'
app.register_blueprint(routes, url_prefix='/')
# CORS(app)
CORS(app,
     origins=["https://fossil-shipment-tracker.appspot.com",
              "https://fossil-shipment-tracker.ew.r.appspot.com/",
              "http://localhost:8080",
              "http://127.0.0.1",
              "https://energyandcleanair.github.io",
              "https://energyandcleanair.org"],
     supports_credentials=True)


@app.teardown_appcontext
def shutdown_session(exception=None):
    session.remove()


@app.errorhandler(Exception)
def exception_handler(err):
    # The error handler for api calls is in routes/__init__.py

    if isinstance(err, HTTPException):
        code = err.code
        response = {
            'message': getattr(err, 'description', code) + " " + str(err)
        }
    else:
        code = 500
        response = {
            'message': err.args[0]
        }
    return jsonify(response), code



if __name__ == "__main__":



    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(debug=True, ssl_context='adhoc', host='127.0.0.1', port=int(os.environ.get('PORT', 8080)))