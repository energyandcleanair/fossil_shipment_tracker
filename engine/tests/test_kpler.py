import datetime as dt
import numpy as np
from base.models import PortCall

from base.db import session, engine

from engine.kpler_scraper import KplerScraper, FlowsSplit, FlowsMeasurementUnit, KplerShip


def test_get_flow():
    scraper = KplerScraper()
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

    flows_kozmino = scraper.get_flows(
        platform="liquids",
        origin_iso2="RU",
        from_installation="Kozmino",
        # destination_iso2="DE",
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
        "origin_iso2": "RU",
        "destination_iso2": "CN",
        "date_from": to_datetime("2022-03-01"),
        "date_to": to_datetime("2022-03-02"),
        "split": FlowsSplit.Products,
        "unit": FlowsMeasurementUnit.T,
    }

    scraper = KplerScraper()
    # flows_brute = scraper.get_flows_raw_brute(**params, include_total=False)
    # flows = scraper.get_flows_raw(**params)
    # assert flows_brute.reset_index(drop=True).equals(flows.reset_index(drop=True))

    params["destination_iso2"] = None
    params["split"] = FlowsSplit.DestinationCountries
    params["product"] = "Crude"
    flows_brute = scraper.get_flows_raw_brute(**params, include_total=False)
    flows = scraper.get_flows_raw(**params)

    flows_brute = flows_brute.sort_values("value", ascending=False)
    flows = flows.sort_values("value", ascending=False)
    flows = flows[flows.value > 0]

    assert flows_brute.split.reset_index(drop=True).equals(flows.split.reset_index(drop=True))
    assert all(np.isclose(flows_brute.value, flows.value, rtol=1e-3))

    # Let's see how far it can go
    params["date_from"] = to_datetime("2013-01-01")
    params["date_to"] = to_datetime("2013-12-31")
    flows_brute = scraper.get_flows_raw_brute(**params, include_total=False)
    assert len(flows_brute.split.unique()) > 20

    flows = scraper.get_flows(**params, use_brute_force=True)

    from engine.kpler_scraper import UNKNOWN_COUNTRY

    assert not all(flows.destination_iso2 == UNKNOWN_COUNTRY)


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
