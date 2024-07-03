from engines import (
    counter,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    counter.update(force=True)
    integrity.check()
    return


if __name__ == "__main__":
    logger_slack.info(
        "=== Update counter forced: using %s environment ===" % (base.db.environment,)
    )
    try:
        update()
        logger_slack.info("=== Update counter forced complete ===")
    except BaseException as e:
        logger_slack.error("=== Update counter forced failed", stack_info=True, exc_info=True)
        raise e
