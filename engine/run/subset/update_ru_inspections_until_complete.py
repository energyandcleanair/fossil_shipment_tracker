from datetime import date
import time
from typing import Optional
from engines import company, insurance, kpler_trade_computed, counter
import integrity
import base


def update():
    while True:
        last_result = company.update(
            max_updates=0,
            steps=[
                company.EquasisUpdateSteps.SHIP_INSPECTIONS,
                company.EquasisUpdateSteps.CLEAN_DATA,
            ],
            filter_departing_iso2s=["RU"],
            filter_minimum_departure_date=date(2022, 1, 1),
        )
        if last_result == company.EquasisUpdateStatus.EQUASIS_EXHAUSTED_FAILURE:
            seconds_in_min = 60
            minutes = 10
            time.sleep(seconds_in_min * minutes)
            company.clear_global_equasis_client()
        else:
            break
    kpler_trade_computed.update()
    counter.update()
    integrity.check()
    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
