from datetime import date
from insurance_scraper import InsuranceScraper

class AmericanSteamshipInsuranceScraper(InsuranceScraper):
    def __init__(self) -> None:
        super().__init__()
    
    def get_imo_date(self, imo: str) -> date:
        return (imo, None)