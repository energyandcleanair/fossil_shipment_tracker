from datetime import datetime
from requests import Session
from bs4 import BeautifulSoup

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

class SkuldInsuranceScraper(InsuranceScraper):
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
        url = 'https://www.skuld.com/vessels/search/'
        
        response = self.session.get(url, params={'q': imo})
        
        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
               
        if '0 hits' in soup.find('p', class_='mb-16 text-base font-light text-text').text:
            logger.info(f"Failed to get date for {imo}: no ship details on page")
            return None
        
        imo_number = soup.find('strong', text='IMO no: ').next_sibling
        
        if imo_number != imo:
            logger.info(f"Failed to get date for {imo}: IMO does not match")
            return None
        
        bunker_date = (soup.find(lambda tag: tag.name == 'strong' and 'BC' in tag.text)
                       .find_next('span').text)
        
        try:
            return datetime.strptime(bunker_date.split(',')[1].split('to')[0].strip(), 
                                     "%d %b %Y")
        except:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None