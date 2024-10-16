from engines import (
    engine_r,
    counter,
    kpler_trade_computed,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    engine_r.update()

    kpler_trade_computed.update()

    counter.update()
    integrity.check()

    return


if __name__ == "__main__":
    logger_slack.info(
        "=== Engine R+dependencies update: using %s environment ===" % (base.db.environment,)
    )
    try:
        update()
        logger_slack.info("=== Engine R+dependencies update complete ===")
    except BaseException as e:
        logger_slack.error(
            "=== Engine R+dependencies update failed", stack_info=True, exc_info=True
        )
        raise e
