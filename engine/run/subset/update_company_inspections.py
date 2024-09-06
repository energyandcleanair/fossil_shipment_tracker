from engines import company, insurance, kpler_trade_computed, counter
import integrity
import base


def update():
    company.update(
        steps=[
            company.EquasisUpdateSteps.SHIP_INSPECTIONS,
            company.EquasisUpdateSteps.CLEAN_DATA,
        ]
    )
    kpler_trade_computed.update()
    counter.update()
    integrity.check()
    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
