from datetime import datetime
from bs4 import BeautifulSoup
from requests import Session

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

class SverigesAngfartysInsuranceScraper(InsuranceScraper):
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
        url = 'https://www.swedishclub.com/vessel/'
        
        response = self.session.get(url, params={'vessel': imo})
        
        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        imo_number = soup.find(text='IMO No:').find_next('div').text.strip()
        
        if imo_number != imo:
            logger.info(f"Failed to get date for {imo}: IMO does not match")
            return None
        
        bunker_date = (soup.find(text='Bunkers Blue Card:')
                       .find_next('div')
                       .text.strip().split(' - ')[0])
        
        try:
            return datetime.strptime(bunker_date, '%d-%b-%Y')
        except:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None