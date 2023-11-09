from datetime import datetime
from bs4 import BeautifulSoup
from requests import Session

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

class SteamshipMutualUnderwritingInsuranceScraper(InsuranceScraper):
    def __init__(self) -> None:
        self.session = Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/53.0.2785.143 Safari/537.36",
                "x-algolia-api-key": "92734b82e4836dfa048ed367bdedd47e",
                "x-algolia-application-id": "866G6FRDI8"
            }
        )
        super().__init__()
    
    def get_insurance_start_date_for_ship(self, imo: str) -> datetime:
        algolia_url = 'https://866g6frdi8-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia for JavaScript (4.8.3); Browser; instantsearch.js (3.7.0); JS Helper (2.28.0)'
        query_url = 'https://www.steamshipmutual.com/node/139461'
        
        response = self.session.post(algolia_url, 
                                     json={"requests":[{"indexName":"prod_vessel","params":f"query={imo}&page=0&highlightPreTag=__ais-highlight__&highlightPostTag=__%2Fais-highlight__&facetFilters=hide_from_search%3Ashow&facets=%5B%5D&tagFilters="}]})
        
        if response.status_code != 200:
            logger.info(
                f"Failed to get date for {imo}: recieved {response.status_code} from server"
            )
            return None
        
        if not response.json()['results'][0]['hits']:
            return None
        
        imo_number = response.json()['results'][0]['hits'][0]['imo_number']
        
        if imo_number != imo:
            logger.info(f"Failed to get date for {imo}: IMO does not match")
            return None
        
        nid = response.json()['results'][0]['hits'][0]['nid']
        
        response = self.session.get(query_url.format(nid=nid))
        soup = BeautifulSoup(response.text, "html.parser")
        
        imo_number = soup.find('p', {'class': 'coh-paragraph right-paragraph'}).text
        
        if imo_number != imo:
            logger.info(f"Failed to get date for {imo}: IMO does not match")
            return None
        
        bunker_date = (soup.find('span', {'class': 'views-label views-label-field-trad-cert-from-date'})
                       .next_sibling.text)
        
        try:
            return datetime.strptime(bunker_date, '%d-%b-%Y')
        except:
            logger.info(f"Failed to get date for {imo}: could not extract date")
            return None