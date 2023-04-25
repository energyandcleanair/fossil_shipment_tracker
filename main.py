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
from engine import kpler_scraper
from callbased import (
    update,
    update_departures,
    update_arrivals,
)
from base.db import init_db
from engine import sts
from engine import backuper
import base

import integrity
import datetime as dt
import callbased


def update():
    # flaring.update()
    company.update()
    # backuper.update()
    # currency.update()
    # rscript.update()
    # kpler_scraper.update_flows("2010-01-01",
    #                            origin_iso2s=["RU"],
    #                            )
    from kpler.sdk import FlowsDirection, FlowsSplit

    kpler_scraper.update_flows(
        destination_iso2s=[
            "IQ",
            "SG",
            "CN",
            "AE",
            "TR",
            "EG",
            "MY",
            "IN",
            "HK",
            "ID",
            "SA",
            "US",
            "IQ",
            "CA",
            "KW",
            "IR",
            "MX",
            "AT",
            "BE",
            "BG",
            "HR",
            "CY",
            "CZ",
            "DK",
            "EE",
            "FI",
            "FR",
            "DE",
            "GR",
            "HU",
            "IE",
            "IT",
            "LV",
            "LT",
            "LU",
            "MT",
            "NL",
            "PL",
            "PT",
            "RO",
            "SK",
            "SI",
            "ES",
            "SE",
            "GB",
            "US",
            "CA",
            "AU",
            "JP",
        ],
        ignore_if_copy_failed=True,
        platforms=["lng"],
        date_from="2013-01-01",
        date_to=dt.date.today(),
        use_brute_force=True,
        add_unknown=True,
        add_unknown_only=False,
        from_splits=[FlowsSplit.OriginCountries],
        to_splits=[FlowsSplit.DestinationCountries],
    )

    # callbased.update_arrivals(commodities=[base.OIL_OR_CHEMICAL],
    #                           date_from='2022-01-01',
    #                           date_to='2023-01-01',
    #                           departure_port_iso2='MY',
    #                           use_credit_key_if_short=True)
    # callbased.get_queried_ship_hours(ship_imo='3442915')
    # counter.update()
    # company.update_info_from_equasis(commodities=['crude_oil'],
    #                                    last_updated='2023-01-23',
    #                                    departure_date_from='2022-12-01')
    # company.fill_country()
    # company.update()
    # currency.update()
    # company.fill_using_imo_website()
    # company.fill_country()
    # counter.update()
    # shipment.update('2021-01-01')
    # position.update_shipment_last_position()
    # destination.update_from_positions()
    # sts.update(date_from=dt.date.today() - dt.timedelta(days=90))
    # for year in [2022]:
    #     print("====== %s ======" % (year))
    #     entsog.update_db(date_from="%s-01-01"%(year),
    #                            date_to="%s-01-31"%(year),
    #                            force=True)

    # shipment.send_diagnostic_chart()
    # entsog.update(date_from="%s-01-01" % (year),
    #               date_to="%s-12-31" % (year),
    #               delete_before_upload=True,
    #               remove_pipe_in_pipe=True,
    #               intermediary_filename='entsog_%s_uploaded_intermediary.csv' % (year),
    #               save_intermediary_to_file=True,
    #               save_to_file=True,
    #               filename='entsog_%s_uploaded.csv' % (year),
    #               )

    # from update_history import update_departures_portcalls, update_arrival_portcalls
    # update_departures(
    #     date_from="2022-01-01",
    #     date_to="2023-01-01",
    #     departure_port_iso2=["HK"],
    # )
    # departure.update(date_from="2021-01-01")
    # company.update()
    # update_arrivals(
    #     date_from="2022-01-01",
    #     date_to="2023-01-01",
    #     departure_port_iso2=["MY"],
    #     commodities=["oil_or_chemical"],
    # )
    # destination.update_matching()
    # destination.update_from_positions()
    # backuper.update()
    # rscript.update()
    # counter.update()
    # ship.fill_missing_commodity()
    return


if __name__ == "__main__":
    # from base.db import init_db
    # init_db(drop_first=False)
    # commodity.fill()
    # country.fill()
    from base.db import init_db

    init_db()
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
