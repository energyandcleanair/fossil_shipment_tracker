import time
from engines import company, insurance, kpler_trade_computed, counter
import integrity
import base


def update():
    last_result = None
    while last_result != company.ComtradeUpdateStatus.SUCCESS:
        last_result = company.update()
        if last_result == company.ComtradeUpdateStatus.ERROR:
            seconds_in_min = 60
            minutes = 30
            time.sleep(seconds_in_min * minutes)
    kpler_trade_computed.update()
    counter.update()
    integrity.check()
    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
