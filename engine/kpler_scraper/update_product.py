import datetime as dt
import json
import os
from base.utils import to_datetime, to_list
from base import UNKNOWN_COUNTRY
from base.models import (
    DB_TABLE_KPLER_PRODUCT,
    DB_TABLE_KPLER_FLOW,
    KplerVessel,
    DB_TABLE_KPLER_TRADE,
)
from base.db_utils import upsert
from base.db import session, engine
from base.logger import logger
import pandas as pd
from tqdm import tqdm
import sqlalchemy as sa
from kpler.sdk import FlowsDirection, FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

from .scraper_product import KplerProductScraper
from .upload import upload_products


def update_products():
    scraper = KplerProductScraper()
    products = scraper.get_products_brute()
    upload_products(products)
