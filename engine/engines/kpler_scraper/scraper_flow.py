import datetime as dt
import requests
import urllib3
import pandas as pd
from kpler.sdk import FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

from ....base.utils import to_datetime, to_list
from ....base.logger import logger

from misc import get_split_name
from scraper import KplerScraper
from scraper_product import KplerProductScraper
from kpler_scraper import KPLER_TOTAL


### IMPORTANT
### Certain country names and to_zone_name are still empty after
### scraping, and have been updated manually in the database
### Before scraping again, add a constraint or a check that this is not the case
###
### Other required fixes:
### - Singapore has duplicates: zone_id 1109 and 833
### - Koweit has duplicates:zone_id 110755 and 505
### Ended up removing 833 and 110755 as having the lowest values
class KplerFlowScraper(KplerScraper):
    def get_flows_raw_brute(
        self,
        platform,
        date_from,
        date_to,
        split,
        from_zone=None,
        to_zone=None,
        product=None,
        unit=None,
        granularity=FlowsPeriod.Daily,
        include_total=True,
    ):
        """
        This one uses the token from the web interface,
        and another payload, that allows us to go back further than 1 year
        :param params:
        :param platform:
        :return:
        """
        if from_zone and from_zone.get("name") == "Unknown":
            return None

        from_locations = (
            [
                self.get_zone_dict(
                    id=from_zone.get("id"), name=from_zone.get("name"), platform=platform
                )
            ]
            if from_zone
            else []
        )

        to_locations = (
            [self.get_zone_dict(id=to_zone.get("id"), name=to_zone.get("name"), platform=platform)]
            if to_zone
            else []
        )

        params_raw = {
            "cumulative": False,
            # "filters": {"product": [1334]},
            "filters": {"product": []},
            "flowDirection": "export",
            # "fromLocations": [{"id": 451, "resourceType": "zone"}],
            "fromLocations": from_locations,
            "toLocations": to_locations,
            "granularity": granularity.value,
            "interIntra": "interintra",
            "onlyRealized": True,
            "view": "kpler",
            "withBetaVessels": False,
            "withForecasted": False,
            "withGrades": True,
            "withIncompleteTrades": True,
            "withIntraCountry": False,
            "vesselClassifications": [],
            "withFreightView": False,
            "withProductEstimation": False,
            "splitOn": split.value,
            "startDate": to_datetime(date_from).strftime("%Y-%m-%d"),
            "endDate": to_datetime(date_to).strftime("%Y-%m-%d"),
        }

        if to_zone is not None:
            params_raw["toLocations"] = [
                self.get_zone_dict(
                    id=to_zone.get("id"), name=to_zone.get("name"), platform=platform
                )
            ]

        if product is not None:
            if isinstance(product, dict):
                params_raw["filters"] = {"product": [int(product.get("id"))]}
            else:
                params_raw["filters"] = {"product": [self.get_product_id(name=product)]}
        else:
            default_products = {"liquids": [1400, 1328, 1370], "lng": [1750], "dry": [1334]}
            params_raw["filters"] = {"product": default_products[platform]}

        token = self.token  # get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/flows",
            "liquids": "https://terminal.kpler.com/api/flows",
            "lng": "https://lng.kpler.com/api/flows",
        }.get(platform)
        headers = {
            "Authorization": f"Bearer {token}",
            "x-web-application-version": "v21.316.0",
            "content-type": "application/json",
        }
        try:
            r = self.session.post(url, json=params_raw, headers=headers)
        except (requests.exceptions.ChunkedEncodingError, urllib3.exceptions.ReadTimeoutError):
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        # read content to dataframe
        try:
            data = r.json()["series"]
        except requests.exceptions.JSONDecodeError:
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        dfs = []
        for x in data:
            df = pd.concat(
                [pd.DataFrame(y["splitValues"]) for y in x["datasets"]], ignore_index=True
            )
            if len(df) > 0:
                df = pd.concat([df.drop(["values"], axis=1), df["values"].apply(pd.Series)], axis=1)
                df["date"] = x["date"]
                dfs += [df]

            # Add total
            # if include_total:
            #     df_total = pd.DataFrame([y["values"] for y in x["datasets"]])
            #     df_total["date"] = x["date"]
            #     df_total["id"] = KPLER_TOTAL
            #     df_total["name"] = KPLER_TOTAL
            #     dfs += [df_total]

        if not dfs:
            return None

        df = pd.concat(dfs, ignore_index=True)
        df.rename(columns={"name": "split_name", "id": "split_id"}, inplace=True)
        df["split"] = df.apply(
            lambda row: {"id": row["split_id"], "name": row["split_name"]}, axis=1
        )
        df.drop(["split_id", "split_name"], axis=1, inplace=True)
        df = df.melt(id_vars=["date", "split"])
        df["date"] = pd.to_datetime(df["date"])

        units = {
            "mass": FlowsMeasurementUnit.T.value,
            "volume": "m3",
            "energy": "GJ?",
        }
        # Recode variable to unit using the dictionary
        df["unit"] = df.variable.map(units)
        df = df[~pd.isna(df.unit)]
        if unit:
            df = df[df.unit == unit.value]
        df.drop(["variable"], axis=1, inplace=True)
        return df

    def get_flows(
        self,
        platform,
        origin_iso2=None,
        destination_iso2=None,
        product=None,
        split=None,
        from_zone=None,
        from_split=FlowsSplit.OriginCountries,
        to_zone=None,
        to_split=FlowsSplit.DestinationCountries,
        granularity=FlowsPeriod.Daily,
        unit=FlowsMeasurementUnit.T,
        date_from=dt.datetime.now() - dt.timedelta(days=365),
        date_to=dt.datetime.now(),
        use_brute_force=True,
    ):
        if from_zone is None and origin_iso2 is not None:
            if from_split == FlowsSplit.OriginCountries:
                from_zone = self.get_zone_dict(platform=platform, iso2=origin_iso2)
            else:
                raise ValueError("Wrong from_zone indication")

        if to_zone is None and destination_iso2 is not None:
            if to_split == FlowsSplit.DestinationCountries:
                to_zone = self.get_zone_dict(platform=platform, iso2=destination_iso2)
            else:
                raise ValueError("Wrong to_zone indication")

        params = {
            "from_zone": from_zone,
            "to_zone": to_zone,
            "product": product,
            "split": split,
            "granularity": granularity,
            "unit": unit,
            "date_from": date_from,
            "date_to": date_to or dt.datetime.now(),
        }

        if use_brute_force:
            df = self.get_flows_raw_brute(platform=platform, **params, include_total=False)
        else:
            df = self.get_flows_raw(platform=platform, **params)
        if df is None:
            return None

        # if destination_iso2 is None and destination_country is not None:
        #     destination_iso2 = self.cc.convert(destination_country, to="ISO2")
        #
        # if destination_iso2 is not None and destination_country is None:
        #     destination_country = self.cc.convert(destination_iso2, to="name_short")

        # Ideally no NULL otherwise the unique constraints won't work
        # This should work from Postgres 15 onwards
        df["from_split"] = get_split_name(from_split)
        df["to_split"] = get_split_name(to_split)
        df["from_iso2"] = origin_iso2 if origin_iso2 else KPLER_TOTAL

        if product is None:
            product_name = KPLER_TOTAL
        elif isinstance(product, str):
            product_name = product
        elif isinstance(product, dict):
            product_name = product.get("name")
        else:
            raise ValueError(f"Unknown product type: {type(product)}")

        df["from_zone"] = df.apply(lambda x: from_zone or {"id": 0, "name": None}, axis=1)
        df["to_zone"] = df.apply(lambda x: to_zone or {"id": 0, "name": None}, axis=1)
        df["product"] = product_name
        df["unit"] = unit.value
        df["platform"] = platform
        df = df.rename(columns={"Date": "date"})

        def split_to_column(df, split):
            if split in [
                FlowsSplit.DestinationCountries,
                FlowsSplit.DestinationInstallations,
                FlowsSplit.DestinationPorts,
            ]:
                df["to_zone"] = df["split"]
            elif split in [
                FlowsSplit.OriginCountries,
                FlowsSplit.OriginInstallations,
                FlowsSplit.OriginPorts,
            ]:
                df["from_zone"] = df["split"]
            elif split in [FlowsSplit.Products, FlowsSplit.Grades]:
                # product is the generic term that is returned by the API
                df["product"] = df["split"].apply(lambda x: x.get("name"))

                df["grade"] = df["split"].apply(
                    lambda x: KplerProductScraper.get_grade_name(platform=platform, id=x.get("id"))
                )
                df["commodity"] = df["split"].apply(
                    lambda x: KplerProductScraper.get_commodity_name(
                        platform=platform, id=x.get("id")
                    )
                )
                df["group"] = df["split"].apply(
                    lambda x: KplerProductScraper.get_group_name(platform=platform, id=x.get("id"))
                )
                df["family"] = df["split"].apply(
                    lambda x: KplerProductScraper.get_family_name(platform=platform, id=x.get("id"))
                )

            return df

        df = split_to_column(df, split)
        df = df.drop(columns=["split"])

        df["from_zone_id"] = df.from_zone.apply(lambda x: self.fix_zone_id(int(x.get("id"))))
        df["to_zone_id"] = df.to_zone.apply(lambda x: self.fix_zone_id(int(x.get("id"))))

        df["to_iso2"] = df.to_zone_id.apply(lambda x: self.get_zone_iso2(platform=platform, id=x))
        df["from_iso2"] = df.from_zone_id.apply(
            lambda x: self.get_zone_iso2(platform=platform, id=x)
        )

        df["from_zone_name"] = df.from_zone.apply(
            lambda x: self.get_zone_name(platform=platform, id=x.get("id"), name=x.get("name"))
        )
        df["to_zone_name"] = df.to_zone.apply(
            lambda x: self.get_zone_name(platform=platform, id=x.get("id"), name=x.get("name"))
        )

        df.drop(columns=["from_zone", "to_zone"], inplace=True)
        return df
