import os
import json
from werkzeug.exceptions import HTTPException
from flask import Flask, request
from flask import jsonify
from flask_cors import CORS
from flask import Response
from flask_mail import Mail, Message

from base.db import session
from base.logger import logger
from base.db import engine


try:
    from .routes import routes
except ImportError:
    from routes import routes

from base.env import get_env

app = Flask(__name__)
app.config.SWAGGER_UI_DOC_EXPANSION = "list"
app.register_blueprint(routes, url_prefix="/")

CORS(
    app,
    origins=[
        "https://fossil-shipment-tracker.appspot.com",
        "https://fossil-shipment-tracker.ew.r.appspot.com/",
        "https://fossil-shipment-tracker.ew.r.appspot.com/",
        "https://fossil-shipment-tracker.ew.r.appspot.com/",
        "http://localhost:8080",
        "http://127.0.0.1",
        "https://energyandcleanair.github.io",
        "https://energyandcleanair.org",
        "https://beyond-coal.eu",
        "*",
    ],
    supports_credentials=True,
)

app.config["MAIL_SERVER"] = "smtp.sendgrid.net"
app.config["MAIL_PORT"] = 587
app.config["MAIL_USE_TLS"] = True
app.config["MAIL_USERNAME"] = "apikey"
app.config["MAIL_PASSWORD"] = get_env("SENDGRID_API_KEY")
app.config["MAIL_DEFAULT_SENDER"] = get_env("MAIL_DEFAULT_SENDER")
mail = Mail(app)


@app.teardown_appcontext
def shutdown_session(exception=None):
    session.remove()


@app.errorhandler(Exception)
def exception_handler(err):
    # The error handler for api calls is in routes/__init__.py

    if isinstance(err, HTTPException):
        code = err.code
        response = {"message": getattr(err, "description", code) + " " + str(err)}
    else:
        code = 500
        response = {"message": err.args[0]}
    return jsonify(response), code


@app.route("/v0/environment", methods=["GET"])
def get_environment():
    from base.db import environment

    return Response(
        response=json.dumps({"environment": environment}), status=200, mimetype="application/json"
    )


@app.route("/_ah/warmup")
def warmup():
    # Handle your warmup logic here, e.g. set up a database connection pool
    logger.info("Warmup call. Connecting to DB")
    engine.connect()
    logger.info("Done")
    return "", 200, {}


@app.route("/v0/update", methods=["POST"])
def update():
    from update import update

    try:
        update()
        return Response(
            response=json.dumps({"status": "OK", "message": "everything updated"}),
            status=200,
            mimetype="application/json",
        )
    except Exception as e:
        return Response(
            response={"status": "ERROR", "message": str(e)}, status=500, mimetype="application/json"
        )


if __name__ == "__main__":
    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(debug=True, host="127.0.0.1", port=int(os.environ.get("PORT", 8080)))
