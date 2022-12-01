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
from  update_history import update_history, update_departures_portcalls, update_arrival_portcalls
from base.db import init_db
import base

import integrity
import datetime as dt


def update():
    # currency.update()
    # entsog.update(date_from=-21, nodata_error_date_from=-4)
    # rscript.update()
    # trajectory.update()
    # flaring.update()
    # portcall.fill_gaps_within_shipments(max_time_delta=dt.timedelta(days=30))
    # arrival.update(date_from='2022-01-01', departure_port_iso2=['RU'],
    #                ship_imo='9085895')
    # currency.update()
    # shipment.send_diagnostic_chart()
    # shipment.rebuild(date_from='2020-10-01')
    # port.update_area()
    # position.update(ship_imo='9373008', date_from='2022-11-06', date_to='2022-11-14')
    # berth.update()
    # counter.update()
    # shipment.update()
    # company.fill_country()
    # position.get_missing_berths(arrival_iso2='EG',
    #                             export_file='missing_berths_egypt.kml',
    #                             hours_from_arrival=24*15,
    #                             cluster_m=50)
    # company.fill_using_imo_website()
    # ship.fill_missing_commodity()
    rscript.update()
    # destination.update_matching()
    # commodity.fill()
    # departure.update('2018-01-01')
    # update_arrival_portcalls(departure_port_iso2='RU',
    #                          commodities=[base.CRUDE_OIL, base.OIL_PRODUCTS, base.LNG],
    #                          date_from='2020-01-01', date_to='2020-01-05')
    # portcall.fill_gaps_within_shipments_using_mtcall(date_from='2022-11-01')
    return


if __name__ == "__main__":
    # from base.db import init_db
    # init_db(drop_first=False)
    # commodity.fill()
    # country.fill()
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
