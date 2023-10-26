from engines import company, insurance, kpler_trade_computed, counter
import integrity
from .. import base


def update():
    company.update()
    insurance.update()
    kpler_trade_computed.update()
    counter.update()
    counter.update(version=base.COUNTER_VERSION1)
    counter.update(version=base.COUNTER_VERSION2)
    integrity.check()
    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
