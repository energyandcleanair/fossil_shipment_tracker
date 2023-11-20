from datetime import datetime
from bs4 import BeautifulSoup
from requests import Session

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

class ShipOwnersMutualScraper(InsuranceScraper):
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
        url = 'https://www.shipownersclub.com/page/controller/lookup/NA/{imo}/vessel'
        
        response = self.session.post(url.format(imo=imo))
        json = response.json()
        
        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None
        
        if not json:
            logger.info(
                f"Failed to get date for {imo}: no matching IMO"
            )
            return None
        
        imo_number = json[0]['imoNumber']
        
        if imo_number != imo:
            logger.info(f"Failed to get date for {imo}: IMO does not match")
            return None
        
        bunker_date = json[0]['startDate']
        
        try:
            return datetime.strptime(bunker_date, '%Y-%m-%dT%H:%M:%S')
        except:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None
        