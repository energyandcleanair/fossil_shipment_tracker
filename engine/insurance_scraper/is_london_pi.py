from datetime import datetime
from requests import Session
from bs4 import BeautifulSoup

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

class LondonPiInsuranceScraper(InsuranceScraper):
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
        base_url = 'https://www.londonpandi.com'
        query_url = 'https://www.londonpandi.com/ship-search/'
        
        response = self.session.get(query_url, params={'q': imo})
        
        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        try:
            ship_url = soup.find('a', href = lambda href: '/ships/' in href)['href']
        except:
            logger.info(f"Failed to get date for {imo}: no ship details on page")
            return None
        
        response = self.session.get(base_url + ship_url) # result redirects to another page
        
        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        ship_details = (soup.find('div', class_='col-md-4 mb-md-2',
                                  text=lambda text: 'IMO' in text)
                        .find_next('div').text)
        
        if not ship_details or (ship_details != imo):
            logger.info(f"Failed to get date for {imo}: no ship details on page or IMO does not match")
            return None
        
        cert_date = (soup.find('td', text=lambda text: 'Certificate' in text)
                     .find_next('td').text)
        
        try:
            return datetime.strptime(cert_date, '%d-%b-%Y')
        except:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None