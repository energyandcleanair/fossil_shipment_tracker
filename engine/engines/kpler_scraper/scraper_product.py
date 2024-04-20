import requests
import urllib3
from requests.adapters import HTTPAdapter, Retry

from base.logger import logger
from base.env import get_env
import pandas as pd

from engines.kpler_scraper.scraper import get_singleton_kpler_client


class KplerProductScraper:
    cache = {}
    client = get_singleton_kpler_client()

    @classmethod
    def get_infos(cls, id):
        if id in KplerProductScraper.cache:
            return KplerProductScraper.cache[id]
        else:
            infos = KplerProductScraper.collect_infos(id=id)
            KplerProductScraper.cache[id] = infos
            return infos

    @classmethod
    def get_parsed_infos(cls, id):
        try:
            infos = cls.get_infos(id=id)
        except Exception as e:
            logger.warning(
                f"Failed with id={id}",
                stack_info=True,
                exc_info=True,
            )
            return None

        if infos is None:
            return None

        return {
            "id": id,
            "name": infos.get("name"),
            "full_name": infos.get("fullName"),
            "type": infos.get("type"),
            "grade_id": cls.get_grade_id(id=id),
            "grade_name": cls.get_grade_name(id=id),
            "commodity_id": cls.get_commodity_id(id=id),
            "commodity_name": cls.get_commodity_name(id=id),
            "group_id": cls.get_group_id(id=id),
            "group_name": cls.get_group_name(id=id),
            "family_id": cls.get_family_id(id=id),
            "family_name": cls.get_family_name(id=id),
        }

    @classmethod
    def get_grade_id(cls, id):
        infos = cls.get_infos(id=id)
        return infos.get("closestAncestorGrade", {}).get("id")

    @classmethod
    def get_grade_name(cls, id):
        infos = cls.get_infos(id=id)
        return infos.get("closestAncestorGrade", {}).get("name")

    @classmethod
    def get_commodity_id(cls, id):
        infos = cls.get_infos(id=id)
        return infos.get("closestAncestorCommodity", {}).get("id")

    @classmethod
    def get_commodity_name(cls, id):
        infos = cls.get_infos(id=id)
        return infos.get("closestAncestorCommodity", {}).get("name")

    @classmethod
    def get_group_id(cls, id):
        infos = cls.get_infos(id=id)
        return infos.get("closestAncestorGroup", {}).get("id")

    @classmethod
    def get_group_name(cls, id):
        infos = cls.get_infos(id=id)
        return infos.get("closestAncestorGroup", {}).get("name")

    @classmethod
    def get_family_id(cls, id):
        infos = cls.get_infos(id=id)
        return infos.get("closestAncestorFamily", {}).get("id")

    @classmethod
    def get_family_name(cls, id):
        infos = cls.get_infos(id=id)
        return infos.get("closestAncestorFamily", {}).get("name")

    @classmethod
    def collect_infos(cls, id):
        try:
            r = KplerProductScraper.client.fetch(f"products/{id}")
        except (requests.exceptions.ChunkedEncodingError, urllib3.exceptions.ReadTimeoutError):
            logger.warning(f"Kpler request failed")
            return None

        return r.json()

    def get_products_brute(self):
        offset = 0
        ids = []

        while offset == 0 or len(r.json()) > 0:
            print(offset)
            r = self.client.fetch("products", params={"size": 1000, "from": offset})
            ids.extend([x.get("id") for x in r.json()])
            offset += 1000
            if offset > 10000:
                break

        products = [self.get_parsed_infos(id=id) for id in ids]
        products = [x for x in products if x is not None]
        products_df = pd.DataFrame(products)

        # Check that each id has only one name
        assert (
            products_df.groupby("id").agg({"name": "nunique"}).reset_index().query("name > 1").empty
        )

        return products_df.to_dict(orient="records")
