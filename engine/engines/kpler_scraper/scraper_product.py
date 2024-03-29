import requests
import urllib3
from requests.adapters import HTTPAdapter, Retry

from base.logger import logger
from base.env import get_env
import pandas as pd


class KplerProductScraper:
    cache = {"liquids": {}, "lng": {}, "dry": {}}
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    token = get_env("KPLER_TOKEN_BRUTE")

    @classmethod
    def get_infos(cls, platform, id):
        if id in KplerProductScraper.cache[platform]:
            return KplerProductScraper.cache[platform][id]
        else:
            infos = KplerProductScraper.collect_infos(platform=platform, id=id)
            KplerProductScraper.cache[platform][id] = infos
            return infos

    @classmethod
    def get_parsed_infos(cls, platform, id):
        try:
            infos = cls.get_infos(platform=platform, id=id)
        except Exception as e:
            logger.warning(
                f"Failed with id={id} and platform={platform}",
                stack_info=True,
                exc_info=True,
            )
            return None

        if infos is None:
            return None

        return {
            "id": id,
            "platform": platform,
            "name": infos.get("name"),
            "full_name": infos.get("fullName"),
            "type": infos.get("type"),
            "grade_id": cls.get_grade_id(platform=platform, id=id),
            "grade_name": cls.get_grade_name(platform=platform, id=id),
            "commodity_id": cls.get_commodity_id(platform=platform, id=id),
            "commodity_name": cls.get_commodity_name(platform=platform, id=id),
            "group_id": cls.get_group_id(platform=platform, id=id),
            "group_name": cls.get_group_name(platform=platform, id=id),
            "family_id": cls.get_family_id(platform=platform, id=id),
            "family_name": cls.get_family_name(platform=platform, id=id),
        }

    @classmethod
    def get_grade_id(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorGrade", {}).get("id")

    @classmethod
    def get_grade_name(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorGrade", {}).get("name")

    @classmethod
    def get_commodity_id(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorCommodity", {}).get("id")

    @classmethod
    def get_commodity_name(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorCommodity", {}).get("name")

    @classmethod
    def get_group_id(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorGroup", {}).get("id")

    @classmethod
    def get_group_name(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorGroup", {}).get("name")

    @classmethod
    def get_family_id(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorFamily", {}).get("id")

    @classmethod
    def get_family_name(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorFamily", {}).get("name")

    @classmethod
    def collect_infos(cls, platform, id):
        token = KplerProductScraper.token  # get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/products",
            "liquids": "https://terminal.kpler.com/api/products",
            "lng": "https://lng.kpler.com/api/products",
        }.get(platform)
        headers = {
            "Authorization": f"Bearer {token}",
            "x-web-application-version": "v21.316.0",
            "content-type": "application/json",
        }
        try:
            r = KplerProductScraper.session.get(f"{url}/{id}", headers=headers)
        except (requests.exceptions.ChunkedEncodingError, urllib3.exceptions.ReadTimeoutError):
            logger.warning(f"Kpler request failed")
            return None

        return r.json()

    def get_products_brute(self, platforms=None):
        if platforms is None:
            platforms = ["liquids", "lng", "dry"]

        products = []
        for platform in platforms:
            url = {
                "dry": "https://dry.kpler.com/api/products",
                "liquids": "https://terminal.kpler.com/api/products",
                "lng": "https://lng.kpler.com/api/products",
            }.get(platform)
            headers = {"Authorization": f"Bearer {self.token}"}
            offset = 0
            ids = []
            while offset == 0 or len(r.json()) > 0:
                print(offset)
                r = self.session.get(url, headers=headers, params={"size": 1000, "from": offset})
                ids.extend([x.get("id") for x in r.json()])
                offset += 1000
                if offset > 10000:
                    break

            products.extend([self.get_parsed_infos(platform=platform, id=id) for id in ids])

        products = [x for x in products if x is not None]
        products_df = pd.DataFrame(products)

        # Check that each id has only one name
        assert (
            products_df.groupby("id").agg({"name": "nunique"}).reset_index().query("name > 1").empty
        )

        # Check that each id has only one platform
        assert (
            products_df.groupby("id")
            .agg({"platform": "nunique"})
            .reset_index()
            .query("platform > 1")
            .empty
        )
        # assert (
        #     len(
        #         products_df.groupby("id")
        #         .agg({"name": "nunique", "platform": "nunique"})
        #         .reset_index()
        #         .query("name > 1 or platform != 3")
        #     )
        #     == 0
        # )

        # Then drop platform
        return products_df.to_dict(orient="records")
