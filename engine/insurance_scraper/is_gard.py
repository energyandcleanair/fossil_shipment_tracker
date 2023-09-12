from datetime import datetime
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

from requests import Session
from bs4 import BeautifulSoup

from base.logger import logger

class GardInsuranceScraper(InsuranceScraper):
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

        url = "https://lov.gard.no/Home/Search?searchbar={}".format(imo)
        response = self.session.get(url)

        if response.status_code != 200:
            status_code = response.status_code
            logger.info(f"Failed to find date for {imo}: site returned status code {status_code}")
            return None

        # if the imo is not found, the url will not redirect
        if "ViewVessel" not in response.url:
            logger.info(f"Failed to find date for {imo}: ship not found")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        header = soup.find("h4", string="Blue Cards and Certificates")

        if not header:
            logger.info(f"Failed to find date for {imo}: could not find blue cards")
            return None

        table = header.parent.find_next_sibling("div")

        items = table.find_all("div")

        for item in items:
            name = item.find("h4")
            date = item.find("span")
            if name and date:
                name = name.text.strip()
                date = date.text.strip()
                if name == "BBC":
                    splits = date.split("Valid from")[-1]
                    dateFrom, _ = splits.split("to")
                    return datetime.strptime(dateFrom.strip(), '%m/%d/%Y')

        logger.info("Failed to find date for {imo}: no BBC in list")
        return None
