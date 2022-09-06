import base
from engine.company import *
from base.env import get_env


def test_equasis_update():
    update_info_from_equasis()


def test_imo_scraper():
    scraper = CompanyImoScraper(
        base_url=base.IMO_BASE_URL,
        service=None
    )

    scraper.initialise_browser()

    if not scraper.perform_login(get_env("IMO_USER"), get_env("IMO_PASSWORD")):
        return False

    expected_info = (
        'Bermuda',
        'Care of SKS Pool AS , Zander Kaaes gate 7, 5015 Bergen, Norway.',
        'SKS SHIPOWNING 9 LTD',
        6269087
    )

    info = scraper.get_information(search_text='6269087')

    assert info == expected_info
