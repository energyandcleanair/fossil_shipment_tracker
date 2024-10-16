from engines import (
    flaring,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    flaring.update()


if __name__ == "__main__":
    logger_slack.info("=== Lite update: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Lite update complete ===")
    except BaseException as e:
        logger_slack.error("=== Lite update failed", stack_info=True, exc_info=True)
        raise e
