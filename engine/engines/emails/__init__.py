from flask import Flask
from flask_mail import Mail, Message

from base.env import get_env

app = Flask(__name__)

app.config["MAIL_SERVER"] = "smtp.sendgrid.net"
app.config["MAIL_PORT"] = 587
app.config["MAIL_USE_TLS"] = True
app.config["MAIL_USERNAME"] = "apikey"
app.config["MAIL_PASSWORD"] = get_env("SENDGRID_API_KEY")
app.config["MAIL_DEFAULT_SENDER"] = get_env("MAIL_DEFAULT_SENDER")
mail = Mail(app)
