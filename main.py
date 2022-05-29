from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import shipment
from engine import trajectory
from engine import position
from engine import destination
from engine import berth
from engine import ship
from engine import country
from engine import counter
from engine import rscript
from engine import commodity
from engine import entsog
from base.db import init_db
import base

import datetime as dt


def update():
    # ship.fix_mmsi_imo_discrepancy(date_from="2022-01-01")
    # ship.fill_missing_commodity()
    # port.add_check_departure_to_anchorage()
    # from base.models import Port
    # from base.db import session
    # import sqlalchemy as sa
    # anc_ports = session.query(Port.marinetraffic_id).filter(sa.and_(
    #     Port.name.op('~*')(' ANCH'),
    #     Port.check_departure
    # )).all()
    # anc_ports = [x[0] for x in anc_ports]
    portcall.update_departures_from_russia(date_from='2021-11-01',
                                           date_to='2022-01-01',
                                           force_rebuild=True,
                                           between_existing_only=True
                                           )
    # portcall.fill_departure_gaps(date_from='2022-04-10')
    # portcall.update_departures_from_russia(date_from='2022-01-01')
    # # #
    # # # # portcall.fill_departure_gaps(date_from="2022-04-10", unlocode='RUULU')
    departure.update(commodities=[base.LNG, base.CRUDE_OIL, base.OIL_PRODUCTS,
                            base.OIL_OR_CHEMICAL, base.COAL, base.BULK],
                     date_from='2021-11-01')

    departure.update(unlocode=['RUVYP', 'RUULU', 'RUMMK', 'RULGA', 'RUVNN', 'RUAZO'],
                     commodities=base.GENERAL_CARGO,
                     date_from='2021-11-01')
    #
    # # # # arrival.update(force_for_arrival_to_departure_greater_than=dt.timedelta(hours=24*10))
    arrival.update(date_from="2021-11-01", include_undetected_arrival_shipments=True)
    # arrival.update(date_from=dt.date.today() - dt.timedelta(days=30))
    shipment.update(date_from='2021-11-01')
    destination.update()
    # berth.fill()
    berth.update()
    # country.fill()
    counter.update()
    entsog.update()
    # ship.collect_mt_for_large_oil_products()
    position.update(date_from='2021-11-01')
    trajectory.update()
    rscript.update()
    counter.update()
    # country.fill()
    return


if __name__ == "__main__":
    # from base.db import init_db
    init_db(drop_first=False)
    # commodity.fill()
    # country.fill()
    print("=== Using %s environment ===" %(base.db.environment,))
    update()
