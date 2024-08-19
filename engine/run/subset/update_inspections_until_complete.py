import time
from typing import Optional
from engines import company, insurance, kpler_trade_computed, counter
import integrity
import base


def update():
    last_result: Optional[company.ComtradeUpdateStatus] = None
    while (
        last_result is None or last_result == company.ComtradeUpdateStatus.EQUASIS_EXHAUSTED_FAILURE
    ):
        last_result = company.update(max_updates=0)
        if last_result == company.ComtradeUpdateStatus.ERROR:
            seconds_in_min = 60
            minutes = 30
            time.sleep(seconds_in_min * minutes)
            company.clear_global_equasis_client()
    kpler_trade_computed.update()
    counter.update()
    integrity.check()
    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
