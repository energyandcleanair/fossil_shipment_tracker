from datetime import datetime
from requests import Session
from bs4 import BeautifulSoup

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper


class BritanniaSteamshipInsuranceScraper(InsuranceScraper):
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
        url = 'https://britanniapandi.com/search-listing/'
        
        response = self.session.get(url, params={'ship': imo})
        
        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        ship_details = soup.find_all(class_='table')[0].find_all('td')[3].text # 1st table; ship info
        
        if not ship_details or (ship_details != imo):
            logger.info(f"Failed to get date for {imo}: no ship details on page or IMO does not match")
            return None
        
        bunker_date = soup.find_all(class_='table')[1].find_all('td')[1].text # 2nd table; blue card details
        
        try:
            return datetime.strptime(bunker_date.strip(), "%B %d, %Y")
        except:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None