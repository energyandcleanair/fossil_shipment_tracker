from datetime import datetime
from requests import Session
from bs4 import BeautifulSoup

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper


class NorthOfEnglandPiInsuranceScraper(InsuranceScraper):
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
        url = 'https://north-standard.com/vessel-search/'
        
        response = self.session.get(url, params={'q': imo})

        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        not_found = soup.find(lambda tag: tag.name == "p" and 'No results found' in tag.text)
        
        if not_found:
            logger.info(f"Failed to get date for {imo}: no ship details on page")
            return None
        
        # imo_found = soup.find(class_ = 'text-sm text-ns-blue break-words', 
        #                       text = lambda text: imo in text)
        
        # if not imo_found:
        #     logger.info(f"Failed to get date for {imo}: IMO does not match")
        #     return None

        bunker_date = (soup.find(class_='font-bold', text=lambda text: 'Bunker' in text)
                       .next_sibling)
        
        try:
            return datetime.strptime((bunker_date.split('From')[1].split('To')[0].strip()), # extract date between 'From' and 'To'
                                     '%d-%b-%Y')
        except:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None