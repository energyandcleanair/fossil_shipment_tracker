import base
from base.logger import logger_slack, post_to_data_channel

from requests import post

import set_rlimit as _

from decouple import config

from google.cloud.secretmanager_v1 import SecretManagerServiceClient
from google.cloud.secretmanager_v1.types import AddSecretVersionRequest, SecretPayload


def update():
    token = get_token()
    post_to_data_channel(f"Kpler token updated: \n```\n{token}\n```")
    update_token(token)


def get_token():
    login_url = "https://terminal.kpler.com/api/login"

    response = post(
        login_url,
        headers={
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        },
        json={
            "email": config("KPLER_EMAIL"),
            "password": config("KPLER_PASSWORD"),
        },
    )

    result = response.json()

    return result["token"]


def update_token(token):
    client = SecretManagerServiceClient()
    project_id = "fossil-shipment-tracker"
    secret_id = "KPLER_TOKEN_BRUTE"
    request = AddSecretVersionRequest(
        parent=f"projects/{project_id}/secrets/{secret_id}",
        payload=SecretPayload(data=token.encode("UTF-8")),
    )

    response = client.add_secret_version(request=request)
    print(response)


if __name__ == "__main__":
    logger_slack.info("=== Refresh kpler token: using %s environment ===")
    try:
        update()
        logger_slack.info("=== Refresh kpler token complete ===")
    except BaseException as e:
        logger_slack.error("=== Refresh kpler token failed", stack_info=True, exc_info=True)
        raise e
