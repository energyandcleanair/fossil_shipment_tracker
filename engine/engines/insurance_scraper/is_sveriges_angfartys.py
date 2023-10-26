from datetime import datetime
from .insurance_scraper import InsuranceScraper


class SverigesAngfartysInsuranceScraper(InsuranceScraper):
    def __init__(self) -> None:
        super().__init__()

    def get_insurance_start_date_for_ship(self, imo: str) -> datetime:
        return None
