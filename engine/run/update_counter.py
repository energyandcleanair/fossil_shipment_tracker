from engines import (
    counter,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    counter.update()
    integrity.check()
    return


if __name__ == "__main__":
    logger_slack.info("=== Update counter only: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Update counter only complete ===")
    except BaseException as e:
        logger_slack.error("=== Update counter only failed", stack_info=True, exc_info=True)
        raise e
