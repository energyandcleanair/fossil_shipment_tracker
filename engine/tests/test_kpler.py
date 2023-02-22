import datetime as dt
from base.models import PortCall

from engine.kpler import KplerScraper, FlowsSplit, FlowsMeasurementUnit


def test_get_flow():
    scraper = KplerScraper()
    # products = scraper.get_products()
    flows = scraper.get_flows(
        origin_iso2="RU", date_from=-10, split=FlowsSplit.Products
    )
    flows = scraper.get_flows(
        origin_iso2="RU", date_from=-10, split=FlowsSplit.DestinationCountries
    )
    assert flows is not None
