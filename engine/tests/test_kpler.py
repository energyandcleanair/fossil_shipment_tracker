import datetime as dt
import numpy as np
import pandas as pd
from kpler.sdk import FlowsDirection, FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

from base.db import session, engine

from engines.kpler_scraper import KplerScraper, KplerFlowScraper, KplerTradeScraper


def test_get_flow():
    scraper = KplerFlowScraper()
    # products = scraper.get_products()
    flows = scraper.get_flows(
        platform="liquids",
        origin_iso2="RU",
        destination_iso2="DE",
        date_from="2022-03-01",
        date_to="2022-03-31",
        split=FlowsSplit.Products,
    )
    assert flows is not None
    sum = flows.groupby("product").sum().sort_values("value", ascending=False).value
    # assert sum.Crude is close to 6E5
    assert round(sum.Crude / 1e4) == 60
    assert round(sum.Diesel / 1e4) == 45
    assert round(sum.Gasoil / 1e4) == 14

    from_zone = scraper.get_zone_dict(name="Kozmino", platform="liquids")
    flows_kozmino = scraper.get_flows(
        platform="liquids",
        origin_iso2="RU",
        from_zone=from_zone,
        date_from="2022-03-01",
        date_to="2022-03-31",
        split=FlowsSplit.DestinationCountries,
    )
    assert flows_kozmino.from_installation.unique() == ["Kozmino"]
    assert flows_kozmino.origin_iso2.unique() == ["RU"]


def test_get_flow_brute():
    from base.utils import to_datetime
    from kpler.sdk import FlowsDirection, FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

    params = {
        "platform": "liquids",
        # "origin_iso2": "RU",
        # "destination_iso2": "CN",
        "date_from": to_datetime("2022-03-01"),
        "date_to": to_datetime("2022-03-02"),
        "split": FlowsSplit.Products,
        "unit": FlowsMeasurementUnit.T,
    }

    scraper = KplerFlowScraper()
    # flows_brute = scraper.get_flows_raw_brute(**params, include_total=False)
    # flows = scraper.get_flows_raw(**params)
    # assert flows_brute.reset_index(drop=True).equals(flows.reset_index(drop=True))
    from_zone = scraper.get_zone_dict(iso2="RU", platform="liquids")
    params["from_zone"] = from_zone
    to_zone = scraper.get_zone_dict(iso2="CN", platform="liquids")
    params["to_zone"] = to_zone

    # params["destination_iso2"] = None
    params["split"] = FlowsSplit.DestinationCountries
    params["product"] = "Crude"
    flows_brute = scraper.get_flows_raw_brute(**params, include_total=False)

    destinations = flows_brute.split.apply(lambda x: x.get("name"))
    assert destinations.unique() == ["China"]


def test_get_installations():
    scraper = KplerScraper()
    # products = scraper.get_products()
    iso2s = ["RU", "CN", "AE"]
    platforms = scraper.platforms
    for iso2 in iso2s:
        for platform in platforms:
            installations = scraper.get_installations(origin_iso2=iso2, platform=platform)
            assert installations is not None
            assert len(installations) > 0


def test_get_vessel_brute():
    scraper = KplerScraper()

    vessel_ids, found_vessels = [104208], []

    for vessel_id in vessel_ids:
        found_vessels.append(scraper.get_vessel_raw_brute(kpler_vessel_id=vessel_id))

    assert len([x for x in found_vessels if x.id in vessel_ids]) == len(vessel_ids)


def test_get_trades():
    scraper = KplerTradeScraper()

    # from_zones = [{"id": "757", "type": "ZONE"}]
    date_from = dt.datetime(2023, 7, 1)
    from_iso2 = ["RU"]
    trades, vessels, zones, products = scraper.get_trades(
        date_from=date_from, from_iso2=from_iso2, platform="liquids", sts_only=True
    )
    assert len(trades) > 0
    assert len(vessels) > 0
    assert len(zones) > 0
    assert len(products) > 0

    trades_df = pd.DataFrame(trades)
    assert not any(pd.isna(trades_df.departure_zone_id))


def test_update_trades():
    from base.db import init_db

    init_db()
    from engines.kpler_scraper import update_trades

    update_trades(date_from=-5)


def test_get_flow_cn():
    from base.utils import to_datetime

    params = {
        "platform": "liquids",
        "origin_iso2": "CN",
        # "destination_iso2": "CN",
        "product": "Crude/Co",
        "date_from": to_datetime("2022-03-20"),
        "date_to": to_datetime("2022-03-24"),
        "split": FlowsSplit.DestinationCountries,
        "unit": FlowsMeasurementUnit.T,
    }

    scraper = KplerFlowScraper()
    flows = scraper.get_flows(**params)
    assert all(flows.from_iso2 == "CN")
    crude = flows[flows["product"] == "Crude/Co"]

    manual_values = {
        "MY": 81900,
        "SG": 15700,
    }
    assert len(crude) == len(manual_values)
    assert all(
        [
            np.isclose(crude[crude.to_iso2 == k].value.iloc[0], v, rtol=1e-2)
            for k, v in manual_values.items()
        ]
    )


def test_get_flow_sg_cn():
    from base.utils import to_datetime

    params = {
        "platform": "liquids",
        "origin_iso2": "SG",
        "destination_iso2": "CN",
        # "product": "Crude",
        "date_from": to_datetime("2019-08-01"),
        "date_to": to_datetime("2019-08-31"),
        "split": FlowsSplit.Products,
        "unit": FlowsMeasurementUnit.T,
    }

    scraper = KplerScraper()
    flows = scraper.get_flows(**params)
    assert all(flows.from_iso2 == "SG")
    assert all(flows.to_iso2 == "CN")
    crude = flows[flows["product"] == "Crude"]

    manual_values = {
        "2019-08-07": 19500,
        "2019-08-10": 15600,
        "2019-08-18": 97700,
    }
    assert len(crude) == len(manual_values)
    assert all(
        [
            np.isclose(crude[crude.date == k].value.iloc[0], v, rtol=1e-2)
            for k, v in manual_values.items()
        ]
    )

    # But there is a date where Singapore != Singapore Republic and the latter seems correct
    params = {
        "platform": "liquids",
        "origin_iso2": "SG",
        "destination_iso2": "CN",
        # "product": "Crude",
        "date_from": to_datetime("2016-06-01"),
        "date_to": to_datetime("2016-06-10"),
        "split": FlowsSplit.Products,
        "unit": FlowsMeasurementUnit.T,
    }

    scraper = KplerScraper()
    flows = scraper.get_flows(**params)
    assert all(flows.from_iso2 == "SG")
    assert all(flows.to_iso2 == "CN")
    crude = flows[flows["product"] == "Crude/Co"]

    manual_values = {
        "2016-06-03": 292000,
    }
    assert len(crude) == len(manual_values)
    assert all(
        [
            np.isclose(crude[crude.date == k].value.iloc[0], v, rtol=1e-2)
            for k, v in manual_values.items()
        ]
    )
