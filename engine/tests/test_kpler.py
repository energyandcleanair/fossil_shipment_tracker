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
