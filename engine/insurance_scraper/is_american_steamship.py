from datetime import datetime
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

from requests import Session
from bs4 import BeautifulSoup

from base.logger import logger

import re


class AmericanSteamshipInsuranceScraper(InsuranceScraper):
    def __init__(self) -> None:
        self.session = Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/53.0.2785.143 Safari/537.36"
            }
        )
        super().__init__()

    def get_insurance_start_date_for_ship(self, imo: str) -> datetime:
        url = f"https://www.american-club.com/page/ship-search/imo-{imo}"

        response = self.session.get(url)

        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        ship_details = soup.find(class_="vesdets")

        if not ship_details:
            logger.info(f"Failed to get date for {imo}: no ship details on page")
            return None

        matches_bunker = lambda element: element.find(class_="detheader").text.startswith(
            "Bunkers CLC"
        )
        bunker_row = next(filter(matches_bunker, ship_details.find_all(class_="search_result")))
        if not bunker_row:
            logger.info(f"Failed to get date for {imo}: no bunker blue card")
            return None

        bunker_dates = bunker_row.find(class_="detdata").text
        if not bunker_dates:
            logger.info(f"Failed to get date for {imo}: no dates for bunker blue card")
            return None

        regex = r"Valid From:\s(?P<from>.*)\sValid To:"

        matches = re.search(regex, bunker_dates)

        if not matches:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None

        matches.groups()

        bunker_date = matches.group("from")

        if not bunker_date:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None

        try:
            return datetime.strptime(bunker_date.strip(), "%B %d, %Y")
        except:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None
