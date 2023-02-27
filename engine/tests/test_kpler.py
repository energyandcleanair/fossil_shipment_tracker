import datetime as dt
from base.models import PortCall

from engine.kpler import KplerScraper, FlowsSplit, FlowsMeasurementUnit


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
    scraper = KplerScraper()
    flows = scraper.get_flows(
        platform="liquids",
        origin_iso2="RU",
        destination_iso2="DE",
        date_from="2022-03-01",
        date_to="2022-03-31",
        split=FlowsSplit.Products,
        use_brute_force=True,
    )
    assert len(flows) > 0


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
