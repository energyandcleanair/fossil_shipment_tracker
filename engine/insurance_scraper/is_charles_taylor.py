from datetime import datetime
from engine.insurance_scraper.insurance_scraper import InsuranceScraper

class CharlesTaylorInsuranceScraper(InsuranceScraper):
    def __init__(self) -> None:
        super().__init__()
    
    def get_insurance_start_date_for_ship(self, imo: str) -> datetime:
        return None