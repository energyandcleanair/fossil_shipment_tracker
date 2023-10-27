from datetime import datetime
from .insurance_scraper import InsuranceScraper

from requests import Session
from bs4 import BeautifulSoup

from base.logger import logger


class StandardClubInsuranceScraper(InsuranceScraper):
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
        url = "https://www.standard-club.com/ship-list/?tx_llcatalog_pi%5Bfilters%5D%5Bkeywords_ships%5D={}".format(
            imo
        )
        response = self.session.get(url, verify=False)

        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        ship_list = soup.find("div", class_="ships list")

        if not ship_list:
            logger.info(f"Failed to get date for {imo}: could not find ship")
            return None

        ship_list_items = ship_list.find("table", class_="simple")

        if not ship_list_items:
            logger.info(f"Failed to get date for {imo}: could not find any items in table")
            return None

        # get last tr in table body
        last_tr = ship_list_items.find_all("tr")[-1]

        if not last_tr:
            return None

        table_list = last_tr.find("ul")

        if not table_list:
            logger.info(
                f"Failed to get date for {imo}: could not find any insurance for ship in ship entry"
            )
            return None

        documents = table_list.find_all("li")

        for document in documents:
            text = document.text.strip()
            if "Bunker" in text:
                _, _, dateFrom, _ = text.split("|")
                return datetime.strptime(dateFrom.strip(), "%d/%m/%Y")

        return None
