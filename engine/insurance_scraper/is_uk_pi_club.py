from datetime import datetime
from bs4 import BeautifulSoup
from requests import Session

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

class UKPiClubInsuranceScraper(InsuranceScraper):
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
        return None