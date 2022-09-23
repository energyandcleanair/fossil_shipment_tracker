from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import shipment
from engine import currency
from engine import company
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
from engine import mtevents
from engine import flaring
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
    # portcall.fill_missing_port_id()

    # portcall.update_departures_from_russia(
    #     marinetraffic_port_id=['24630','24631'],
    #     date_from='2022-01-01',
    #     date_to='2022-05-01',
    #     force_rebuild=True,
    #     between_existing_only=False)
    #
    # portcall.update_departures_from_russia(
    #     marinetraffic_port_id=['24630','24631'],
    #     date_from='2022-05-01',
    #     force_rebuild=True,
    #     between_existing_only=False)

    # departure.update(port_id=115225)
    #
    # departure.update(commodities=[base.LNG, base.CRUDE_OIL, base.OIL_PRODUCTS,
    #                               base.OIL_OR_CHEMICAL, base.COAL, base.BULK])
    #
    # departure.update(unlocode=['RUVYP', 'RUULU', 'RUMMK', 'RULGA', 'RUVNN', 'RUAZO'],
    #                  commodities=base.GENERAL_CARGO)
    #
    # # Only keep oil related for Sikka (India) and MERSA EL HAMRA (Egypt)
    # departure.remove(unlocode=['INSIK'],
    #                  port_id=114313,
    #                  commodities=[base.LNG, base.COAL, base.BULK])
    #
    # departure.remove(port_name='SIKKA ANCH',
    #                  commodities=[base.LNG, base.COAL, base.BULK])
    #
    # departure.remove(unlocode=['EGMAH'],
    #                  commodities=[base.LNG, base.COAL, base.BULK])
    #
    # departure.remove(port_name='MERSA EL HAMRA ANCH',
    #                  commodities=[base.LNG, base.COAL, base.BULK])

    # departure.update(date_from='2022-01-01',
    #                  unlocode='EGMAH',
    #                  commodities=[base.CRUDE_OIL, base.OIL_OR_CHEMICAL, base.OIL_PRODUCTS])
    # Only keep oil related for India
    # departure.remove(unlocode=['INSIK'],
    #                  commodities=[base.LNG, base.COAL, base.BULK])
    #
    # departure.remove(port_name='SIKKA ANCH',
    #                  commodities=[base.LNG, base.COAL, base.BULK])
    # port.add_check_departure_to_anchorage()

    indian_port_ids = [
            114313, #SIKKA ANCH
            62343, #SIKKA
            61757, #JAMNAGAR
            62257 #Reliance SEZ/Jamnagar
        ]

    # departure.update(date_from='2021-01-01')
    # portcall.update_departures_from_russia(
    #     port_id=indian_port_ids,
    #     date_from='2021-10-01',
    #     force_rebuild=True,
    #     between_existing_only=True)
    #
    # departure.update(commodities=[base.CRUDE_OIL, base.OIL_PRODUCTS, base.OIL_OR_CHEMICAL],
    #                  date_from='2021-10-01',
    #                  port_id=indian_port_ids)
    #
    # # departure.remove(port_id=indian_port_ids,
    # #                  commodities=[base.LNG, base.COAL, base.BULK])
    #
    # arrival.update(port_id=indian_port_ids,
    #                commodities=[base.CRUDE_OIL, base.OIL_PRODUCTS, base.OIL_OR_CHEMICAL],
    #                date_from='2021-10-01')
    #
    # shipment.update(date_from='2021-10-01')


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
    # arrival.update(date_from='2022-01-01', port_id=[30915, 115225]) #MERSA EL HAMRA ANCH
    # # ship.fill_missing_insurer()
    # # currency.update(date_from='2015-01-01', date_to='2020-01-01', force=True)
    # shipment.update()
    # position.update(imo='9217321')
    # destination.update()
    # berth.update()
    # entsog.update(date_from=-14)
    # flows = entsog.get_flows_by_pointtype(date_from='2020-01-01',
    #                                       date_to='2020-12-31',
    #                                       country_iso2='DE')
    # flows.value_m3.sum() / 1e9
    # flows['bcm'] = flows.value_m3 / 1e9
    # flows['bcm'] = flows.bcm.round(1)
    #
    # flows2 = entsog.get_flows_by_pointtype(date_from='2020-01-01',
    #                                       date_to='2020-12-31',
    #                                       country_iso2='DE',
    #                                        remove_pipe_in_pipe=False)
    # flows2.value_m3.sum() / 1e9
    # rscript.update()
    # trajectory.update()
    # counter.update(date_from='2021-11-01')
    # ship.fix_not_found()
    # arrival.update(date_from=dt.date.today() - dt.timedelta(days=90),
    #                ship_imo='9487677')
    # arrival.update(date_from=dt.date.today() - dt.timedelta(days=180))
    # currency.update()
    # shipment.update()
    # portcall.fill_missing_port_id()
    # company.fill_country()
    # commodity.fill()
    # country.fill()
    # company.update()
    # portcall.fill_missing_port_id()
    # position.update()
    # mtevents.update(date_from='2020-11-01', force_rebuild=True,
    #                 commodities=[
    #                     # base.LNG,
    #                     base.CRUDE_OIL,
    #                     base.OIL_PRODUCTS,
    #                     # base.OIL_OR_CHEMICAL
    #                 ],
    #                 )
    # shipment.rebuild('2020-07-01')
    # destination.update()
    # position.get_missing_berths()
    # berth.update()
    # entsog.update(date_from=-21, nodata_error_date_from=-4)
    # rscript.update()
    # trajectory.reroute()
    # trajectory.reroute(shipment_id=[528612, 490038])
    # trajectory.update(shipment_id=359731)
    # berth.update()
    # alert.update()
    # counter.update(date_from='2021-01-01')
    flaring.update()
    # berth.update()
    # commodity.fill()
    # flaring.update()
    return


if __name__ == "__main__":
    # from base.db import init_db
    # init_db(drop_first=False)
    # commodity.fill()
    # country.fill()
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
