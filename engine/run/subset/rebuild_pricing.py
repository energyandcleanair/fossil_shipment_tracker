from engines import (
    engine_r,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    engine_r.update(rebuild_prices=True)
    return


if __name__ == "__main__":
    logger_slack.info("=== Rebuild pricing: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Rebuild pricing complete ===")
    except BaseException as e:
        logger_slack.error("=== Rebuild pricing failed", stack_info=True, exc_info=True)
        raise e
