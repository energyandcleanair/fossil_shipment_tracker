import logging
from slack_logger import SlackHandler, SlackFormatter
from base.env import get_env
from slack_sdk import WebClient
import requests


# General logging parameters
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logging.getLogger("urllib3.connectionpool").setLevel(level=logging.WARNING)
logging.getLogger("fiona").setLevel(level=logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(level=logging.WARNING)
logging.getLogger("sqlalchemy.engine.base.Engine").setLevel(level=logging.WARNING)
logging.getLogger("google.auth").setLevel(level=logging.WARNING)
logging.getLogger("urllib3").setLevel(level=logging.WARNING)
logging.getLogger("botocore").setLevel(level=logging.WARNING)
logging.getLogger("shapely").setLevel(level=logging.WARNING)
logging.getLogger("country_converter.country_converter").setLevel(level=logging.ERROR)

logger = logging.getLogger("FOSSIL_SHIPMENT_TRACKER")
logger.setLevel(logging.INFO)


# Adding slack logger
# One handler to send critical messages
# Another one to send Slack specific messages
def slack_webhook_ok(url):
    if url is None:
        return False
    if url == "":
        return False
    r = requests.post(url)
    return r.status_code == 400  # Would be 403 if address is invalid


logger_slack = logging.getLogger("FOSSIL_SHIPMENT_TRACKER_SLACK")
logger_slack.setLevel(logging.INFO)

slack_error_handler = None
if slack_webhook_ok(get_env("SLACK_WEBHOOK")):
    slack_error_handler = SlackHandler(get_env("SLACK_WEBHOOK"))
    slack_error_handler.setFormatter(SlackFormatter())
    slack_error_handler.setLevel(level=logging.ERROR)

    logger.addHandler(slack_error_handler)

    direct_slack_handler = SlackHandler(get_env("SLACK_WEBHOOK"))
    direct_slack_handler.setFormatter(SlackFormatter())
    direct_slack_handler.setLevel(level=logging.INFO)
    logger_slack.addHandler(direct_slack_handler)

slack_token = get_env("SLACK_API_TOKEN", None)
slack_enabled = slack_token != None and slack_token != ""

slacker = WebClient(token=slack_token)

USERS = ["<@U012ZQ5NU4U>", "<@U05LD5C42G6>"]  # Hubert  # Panda
NOTIFICATION_STRING = " ".join(USERS)
CHANNEL = "#log-russia-counter"
TOKEN_CHANNEL = "#project-russia-kpler-token"


def notify_engineers(msg):
    if slack_enabled:
        slacker.chat_postMessage(channel=CHANNEL, text=f"{msg}")
    else:
        logger.warning("Slack logging disabled. Did not notify engineers.")


def post_to_token_channel(msg):
    if slack_enabled:
        slacker.chat_postMessage(channel=TOKEN_CHANNEL, text=msg)
