from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import shipment
from engine import currency
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
from engine import alert
from base.db import init_db
import base

import datetime as dt


def update():
    # ship.fix_mmsi_imo_discrepancy(date_from="2022-01-01")
    # ship.fill_missing_commodity()
    # ship.collect_mt_for_insurers()
    # ship.collect_equasis_for_additional_infos()
    # port.add_check_departure_to_anchorage()
    # from base.models import Port
    # from base.db import session
    # import sqlalchemy as sa
    # anc_ports = session.query(Port.marinetraffic_id).filter(sa.and_(
    #     Port.name.op('~*')(' ANCH'),
    #     Port.check_departure
    # )).all()
    # anc_ports = [x[0] for x in anc_ports]
    #
    # Only keep oil related for India
    # departure.remove(unlocode=['INSIK'],
    #                  commodities=[base.LNG, base.COAL, base.BULK])
    #
    # departure.remove(port_name='SIKKA ANCH',
    #                  commodities=[base.LNG, base.COAL, base.BULK])

    # portcall.update_departures_from_russia(
    #     unlocode=['INSIK'],
    #     date_from='2022-02-24',
    #     # date_to='2022-01-01',
    #     force_rebuild=True,
    #     between_existing_only=True)
    #
    # departure.update(commodities=[base.CRUDE_OIL, base.OIL_PRODUCTS],
    #                  date_from='2022-02-24',
    #                  unlocode=['INJGA', 'INSIK'])
    #
    # portcall.update_departures_from_russia(date_from='2021-01-01',
    #                                         date_to='2021-03-01',
    #                                         force_rebuild=True,
    #                                         between_existing_only=False
    #                                         )
    # portcall.fill_departure_gaps(date_from='2022-04-10')
    # portcall.update_departures_from_russia(date_from='2022-01-01')
    # # #
    # # # # portcall.fill_departure_gaps(date_from="2022-04-10", unlocode='RUULU')
    # departure.update(commodities=[base.LNG, base.CRUDE_OIL, base.OIL_PRODUCTS,
    #                               base.OIL_OR_CHEMICAL, base.COAL, base.BULK],
    #                  date_from='2021-01-01')

    # departure.update(unlocode=['RUVYP', 'RUULU', 'RUMMK', 'RULGA', 'RUVNN', 'RUAZO'],
    #                   commodities=base.GENERAL_CARGO,
    #                   date_from='2021-01-01')
    # # #
    # # # # # # arrival.update(force_for_arrival_to_departure_greater_than=dt.timedelta(hours=24*10))
    # arrival.update(date_from='2021-01-01',
    #                date_to='2021-10-31',
    #                include_undetected_arrival_shipments=True)
    # ship.fill_missing_insurer()
    # currency.update(date_from='2021-11-01', date_to='2022-01-01', force=True)
    # shipment.update()
    # position.update()
    # destination.update()
    # berth.update()
    # entsog.update(date_from=-14)
    # rscript.update()
    # trajectory.update()
    # counter.update(date_from='2021-11-01')
    # ship.fix_not_found()
    # # arrival.update(date_from=dt.date.today() - dt.timedelta(days=30))
    # shipment.rebuild(date_from='2021-11-01')
    # shipment.rebuild()
    # shipment.update()
    # destination.update()
    # # berth.fill()
    # berth.detect_arrival_berths()
    # # # country.fill()
    # counter.update()
    # trajectory.reroute(date_from='2021-01-01')
    # # # ship.collect_mt_for_large_oil_products()
    # position.update(shipment_id=255262)
    # trajectory.update()
    # berth.update(shipment_id=255262)
    # rscript.update()
    counter.update()
    # alert.update()
    # country.fill()
    # berth.update()
    # entsog.update(date_from='2022-02-24')
    # rscript.update()
    # trajectory.update()
    # counter.update()
    return


if __name__ == "__main__":
    # from base.db import init_db
    init_db(drop_first=False)
    # commodity.fill()
    # country.fill()
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
