from .scraper_product import KplerProductScraper
from .upload import upload_products


def update_products():
    scraper = KplerProductScraper()
    products = scraper.get_products_brute()
    upload_products(products)
