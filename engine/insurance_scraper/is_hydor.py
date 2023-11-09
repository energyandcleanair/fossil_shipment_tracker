from datetime import datetime
from bs4 import BeautifulSoup
from requests import Session

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

class HydorASInsuranceScraper(InsuranceScraper):
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
        url = 'https://hydor-vesselsearch.herokuapp.com/vessels/index'
        payload = f'vessel_search%5Bquery%5D={imo}'
        
        response = self.session.post(url, data=payload)
        
        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        no_result = (soup.find('div', {'class': 'col-lg-6 mx-auto alert alert-danger fade show text-center'})
                     .text)
        
        if 'no results' in no_result:
            logger.info(f"Failed to get date for {imo}: no ship details on page")
            return None
        
        imo_number = soup.find(text=lambda x: 'IMO:' in x.text).next_element.strip()
        
        if imo_number != imo or not imo_number:
            logger.info(f"Failed to get date for {imo}: IMO does not match")
            return None
        
        bunker_date = (soup.find(text=lambda x: 'Inception Date:' in x.text)
                       .next_element.strip())
        
        try:
            return datetime.strptime(bunker_date, '%d %b %Y')
        except:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None