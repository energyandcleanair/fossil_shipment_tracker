from datetime import datetime
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

import requests
from bs4 import BeautifulSoup

from base.logger import logger

base_url = "https://www.westpandi.com/vessels?search="


class WestOfEnglandInsuranceScraper(InsuranceScraper):
    def __init__(self) -> None:
        super().__init__()

    def get_insurance_start_date_for_ship(self, imo: str) -> datetime:
        try:
            response = requests.get(base_url + imo)
        except requests.exceptions.HTTPError as e:
            self._log("HTTP error")
            raise e

        html = BeautifulSoup(response.content, "html.parser")

        blue_card_elements = html.find_all(class_="blue-card")

        matches_bunker = (
            lambda element: element.find(class_="blue-card__name").getText().strip() == "Bunkers"
        )

        matching_blue_card_el = next(filter(matches_bunker, blue_card_elements), None)

        if matching_blue_card_el == None:
            logger.info(f"Failed to find date for {imo}: missing blue card for 'Bunkers'")
            return None

        dates = matching_blue_card_el.find(class_="blue-card__date").getText().strip()

        if not dates or dates == "":
            logger.info(f"Failed to find date for {imo}: no date for 'Bunkers'")
            return None

        start_date_string = dates.split("-")[0].strip()

        try:
            return datetime.strptime(start_date_string, "%m/%d/%Y")
        except ValueError:
            logger.info(f"Failed to find date for {imo}: unable to parse date {start_date_string}")
            return None
