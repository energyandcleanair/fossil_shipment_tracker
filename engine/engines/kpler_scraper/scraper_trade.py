from typing import List
from tqdm import tqdm

import datetime as dt

import base
from .scraper import *
from .scraper_product import KplerProductScraper
from .misc import get_nested

from base.utils import *


class KplerTradeScraper(KplerScraper):
    def __init__(self):
        super().__init__()

    def get_trades(self, platform, from_iso2=None, date_from=-30, sts_only=False, month=None):
        if sts_only:
            operational_filter = "shipToShip"
        else:
            operational_filter = None

        trades = []
        vessels = []
        zones = []
        products = []
        installations = []

        for current_iso2 in to_list(from_iso2):
            date_from = to_datetime(date_from)
            from_zone = self.get_zone_dict(iso2=current_iso2, platform=platform)
            trades_raw = []

            # We split by group to be able to go further back in time (otherwise hitting 10000 window)
            products_ = pd.DataFrame(self.get_products())
            if platform == "liquids":
                products_ = products_[products_["type"] == "group"]
            else:
                products_ = products_[products_["type"] == "commodity"]
            products_ = products_[products_["platform"] == platform]
            product_ids = products_["id"].tolist()

            for product_id in product_ids:
                query_from = 0
                while True:
                    size, query_trades_raw = self.get_trades_raw(
                        from_zone=from_zone,
                        platform=platform,
                        query_from=query_from,
                        operational_filter=operational_filter,
                        product_ids=[product_id],
                        month=month,
                    )
                    trades_raw.extend(query_trades_raw)
                    query_from += size
                    if (
                        size == 0
                        or min([pd.to_datetime(x.get("start")) for x in query_trades_raw])
                        < date_from
                    ):
                        break

            for x in tqdm(trades_raw, unit="raw-trade", leave=False):
                trades_, vessels_, zones_, products_, installations_ = self._parse_trade(
                    x, platform=platform
                )
                trades.extend(trades_)
                vessels.extend(vessels_)
                zones.extend(zones_)
                products.extend(products_)
                installations.extend(installations_)

        return trades, vessels, zones, products, installations

    def get_trades_raw(
        self,
        platform,
        from_zone=None,
        query_from=0,
        product_ids=None,
        operational_filter=None,
        month=None,
    ):
        if from_zone and from_zone.get("name") == "Unknown":
            return 0, []

        from_locations = (
            [
                self.get_zone_dict(
                    id=from_zone.get("id"), name=from_zone.get("name"), platform=platform
                )
            ]
            if from_zone
            else []
        )

        filters = {}

        if product_ids is not None:
            filters["product"] = product_ids

        # Get zone dict
        params_raw = {
            "exchangeType": "export",
            "filters": filters,
            "flowDirection": "export",
            "from": query_from,
            "fromLocations": from_locations,
            "interIntra": "interintra",
            "locations": [],
            "onlyRealized": False,
            "players": [],
            "size": 1000,
            "splitValues": [],
            "splitValuesToExclude": [],
            "toLocations": [],
            "vesselClassifications": [],
            "vessels": [],
            "view": "kpler",
            "withBetaVessels": False,
            "withForecasted": True,
            "withFreightView": False,
            "withIncompleteTrades": True,
            "withIntraCountry": True,
            "withProductEstimation": False,
        }

        if month:
            params_raw["granularity"] = "months"
            params_raw["period"] = month

        if product_ids is not None:
            params_raw["products"] = to_list(product_ids)

        token = self.token  # get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/flows/trades",
            "liquids": "https://terminal.kpler.com/api/flows/trades",
            "lng": "https://lng.kpler.com/api/flows/trades",
        }.get(platform)

        headers = {
            "Authorization": f"Bearer {token}",
            "x-web-application-version": "v21.316.0",
            "content-type": "application/json",
        }
        try:
            r = requests.post(url, json=params_raw, headers=headers)
        except requests.exceptions.ChunkedEncodingError:
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            raise

        if r.status_code not in [200, 204]:
            raise RuntimeError(
                f"Kpler request failed: {params_raw}. Status code: {r.status_code}, {r.content}"
            )

        try:
            trades_raw = r.json()
        except (json.decoder.JSONDecodeError, requests.JSONDecodeError, requests.RequestException):
            if "Result window is too large" in r.text:
                logger.warning(
                    f"Reached the end for Kpler trade. Probably required to split by product even further"
                )
                raise
            else:
                logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
                raise
        return len(trades_raw), trades_raw

    def _parse_trade_sts(self, x, origin_or_destination):
        sts = {}
        if origin_or_destination == "origin":
            sts_raw = get_nested(x, "portCallOrigin", "shipToShipInfo")
        elif origin_or_destination == "destination":
            sts_raw = get_nested(x, "portCallDestination", "shipToShipInfo")
        else:
            return None

        if sts_raw is None:
            return None

        sts["id"] = sts_raw.get("id")
        sts["start"] = sts_raw.get("start")
        sts["end"] = sts_raw.get("end")
        sts["zone_id"] = get_nested(sts_raw, "zone", "id")
        sts["zone_name"] = get_nested(sts_raw, "zone", "name")
        sts["zone_fullname"] = get_nested(sts_raw, "zone", "fullname")
        sts["zone_country"] = get_nested(sts_raw, "zone", "country", "name")
        sts["origin_or_destination"] = origin_or_destination

        sts["mass"] = get_nested(sts_raw, "shipToShipQuantity", "mass")

        flows = get_nested(sts_raw, "flowQuantities")
        if len(flows) > 1:
            logger.warning(f"More than one flow in trade {x.get('id')}. Not managed yet.")
            return None
        elif len(flows) == 0:
            return None

        flow = flows[0]
        sts["product_id"] = flow.get("id")
        sts["product_name"] = flow.get("name")
        return sts

    def _parse_trade_vessels(self, vessels) -> List[dict]:
        return [
            {
                "id": x.get("id"),
                "name": x.get("name"),
                "imo": x.get("imo"),
                "mmsi": x.get("mmsi"),
                "dwt": x.get("deadWeight"),
                "others": x,
            }
            for x in vessels
        ]

    def _parse_trade_trade(self, trade_raw, platform) -> List[dict]:
        trade = {}
        # General
        trade["id"] = trade_raw.get("id")
        trade["platform"] = platform
        status_dict = {
            "In Transit": base.ONGOING,
            "Delivered": base.COMPLETED,
        }
        if trade_raw.get("status") not in status_dict:
            return []
        else:
            trade["status"] = status_dict[trade_raw.get("status")]

        trade["departure_date_utc"] = pd.to_datetime(trade_raw.get("start"))
        trade["arrival_date_utc"] = pd.to_datetime(trade_raw.get("end"))

        # Zones
        trade["departure_zone_id"] = get_nested(
            trade_raw, "portCallOrigin", "zone", "id", warn=False
        )
        # trade["departure_zone_name"] = get_nested(
        #     trade_raw, "portCallOrigin", "zone", "name", warn=False
        # )
        # trade["departure_zone_type"] = get_nested(
        #     trade_raw, "portCallOrigin", "zone", "type", warn=False
        # )
        # trade["departure_iso2"] = self._country_name_to_iso2(
        #     get_nested(trade_raw, "portCallOrigin", "zone", "country", "name")
        # )

        trade["arrival_zone_id"] = get_nested(
            trade_raw, "portCallDestination", "zone", "id", warn=False
        )
        # trade["arrival_zone_name"] = get_nested(
        #     trade_raw, "portCallDestination", "zone", "name", warn=False
        # )
        # trade["arrival_zone_type"] = get_nested(
        #     trade_raw, "portCallDestination", "zone", "type", warn=False
        # )
        # trade["arrival_iso2"] = self._country_name_to_iso2(
        #     get_nested(trade_raw, "portCallDestination", "zone", "country", "name", warn=False)
        # )

        # Berth
        trade["departure_berth_id"] = get_nested(
            trade_raw, "portCallOrigin", "berth", "id", warn=False
        )
        trade["departure_berth_name"] = get_nested(
            trade_raw, "portCallOrigin", "berth", "name", warn=False
        )
        trade["arrival_berth_id"] = get_nested(
            trade_raw, "portCallDestination", "berth", "id", warn=False
        )
        trade["arrival_berth_name"] = get_nested(
            trade_raw, "portCallDestination", "berth", "name", warn=False
        )

        # Installation
        trade["departure_installation_id"] = get_nested(
            trade_raw, "portCallOrigin", "installation", "id", warn=False
        )
        trade["departure_installation_name"] = get_nested(
            trade_raw, "portCallOrigin", "installation", "name", warn=False
        )
        trade["arrival_installation_id"] = get_nested(
            trade_raw, "portCallDestination", "installation", "id", warn=False
        )
        trade["arrival_installation_name"] = get_nested(
            trade_raw, "portCallDestination", "installation", "name", warn=False
        )

        trade["departure_sts"] = get_nested(trade_raw, "portCallOrigin", "shipToShip")
        trade["arrival_sts"] = get_nested(
            trade_raw, "portCallDestination", "shipToShip", warn=False
        )

        # Vessels
        trade["vessel_ids"] = [y.get("id") for y in trade_raw.get("vessels")]
        trade["vessel_imos"] = [y.get("imo") for y in trade_raw.get("vessels")]

        # Steps i.e. StS
        trade["step_zone_ids"] = [y.get("zone").get("id") for y in trade_raw.get("steps")] or None

        # Flows
        flows = self._parse_trade_flows(trade_raw)

        # Players
        buyers = (
            get_nested(trade_raw, "orgSpecificInfo", "default", "confirmedBuyers", warn=False) or []
        )
        sellers = (
            get_nested(trade_raw, "orgSpecificInfo", "default", "confirmedSellers", warn=False)
            or []
        )
        trade["buyer_ids"] = [x.get("id") for x in buyers]
        trade["buyer_names"] = [x.get("name") for x in buyers]
        trade["seller_ids"] = [x.get("id") for x in sellers]
        trade["seller_names"] = [x.get("name") for x in sellers]

        trade["is_valid"] = True

        # Do a cross product of all flows with trade
        result = []
        for flow in flows:
            flow.pop("trade_id")
            trade_copy = trade.copy()
            trade_copy.update(flow)
            result.append(trade_copy)

        return result

    def _parse_trade_flows(self, trade_raw) -> List[dict]:
        trade_id = trade_raw.get("id")
        flows_raw = trade_raw.get("flowQuantities")
        if len(flows_raw) == 0:
            return []

        flows = []
        for flow_raw in flows_raw:
            flow = {}
            flow["trade_id"] = trade_id
            flow["flow_id"] = flow_raw.get("id")
            flow["product_id"] = flow_raw.get("confirmedProduct").get("productId")
            # flow["product_name"] = flow_raw.get("confirmedProduct").get("name")
            # flow["product_type"] = flow_raw.get("confirmedProduct").get("type")
            flow["value_tonne"] = flow_raw.get("flowQuantity").get("mass")
            flow["value_m3"] = flow_raw.get("flowQuantity").get("volume")
            # Looks like GJ but not 100% sure
            flow["value_energy"] = flow_raw.get("flowQuantity").get("energy")
            flow["value_gas_m3"] = flow_raw.get("flowQuantity").get("volume_gas")
            flows += [flow]

        return flows

    def _parse_trade_zones(self, trade_raw) -> List[dict]:
        """
        Extract all possible information from trade_raw about zones,
        be it berth, port, or country
        :param trade_raw:
        :return:
        """

        if not trade_raw:
            return []

        zones = [
            get_nested(trade_raw, "portCallOrigin", "zone", warn=False),
            get_nested(trade_raw, "portCallDestination", "zone", warn=False),
        ]

        # Adding steps
        zones += [y.get("zone") for y in trade_raw.get("steps")]

        result = []
        for zone in zones:
            if not zone:
                continue

            primary_zone = {}
            primary_zone["id"] = zone.get("id")
            primary_zone["name"] = zone.get("name")
            primary_zone["type"] = zone.get("type")

            port = next((x for x in zone.get("parentZones", []) if x["isPort"]), {})
            country = zone.get("country") or {}

            primary_zone["port_id"] = (zone.get("port") or {}).get("id")
            primary_zone["port_name"] = (zone.get("port") or {}).get("name")
            primary_zone["country_id"] = (zone.get("country") or {}).get("id")
            primary_zone["country_name"] = (zone.get("country") or {}).get("name")
            primary_zone["country_iso2"] = self._country_name_to_iso2(primary_zone["country_name"])
            result.append(primary_zone)

            if port:
                port_zone = {}
                port_zone["id"] = port.get("id")
                port_zone["name"] = port.get("name")
                port_zone["type"] = port.get("type")
                port_zone["port_id"] = port.get("id")
                port_zone["port_name"] = port.get("name")
                port_zone["country_id"] = primary_zone["country_id"]
                port_zone["country_name"] = primary_zone["country_name"]
                port_zone["country_iso2"] = primary_zone["country_iso2"]
                result.append(port_zone)

            if country:
                country_zone = {}
                country_zone["id"] = country.get("id")
                country_zone["name"] = country.get("name")
                country_zone["type"] = country.get("type")
                country_zone["country_id"] = primary_zone["country_id"]
                country_zone["country_name"] = primary_zone["country_name"]
                country_zone["country_iso2"] = primary_zone["country_iso2"]
                result.append(country_zone)

        # Remove duplicates
        result = [dict(t) for t in {tuple(d.items()) for d in result}]
        return result

    def _parse_trade_installations(self, trade_raw) -> List[dict]:
        """
        Extract all possible information from trade_raw about zones,
        be it berth, port, or country
        :param trade_raw:
        :return:
        """

        if not trade_raw:
            return []

        installations = [
            get_nested(trade_raw, "portCallOrigin", "installation", warn=False),
            get_nested(trade_raw, "portCallDestination", "installation", warn=False),
        ]

        result = []
        for installation_raw in installations:
            if not installation_raw:
                continue

            installation = {}
            installation["id"] = installation_raw.get("id")
            installation["name"] = installation_raw.get("name")
            installation["fullname"] = installation_raw.get("fullname")
            installation["type"] = installation_raw.get("type")
            installation["port_id"] = (installation_raw.get("port") or {}).get("id")
            result.append(installation)

        # Remove duplicates
        result = [dict(t) for t in {tuple(d.items()) for d in result}]
        return result

    def _parse_trade_products(self, flow, platform) -> List[dict]:
        if not flow:
            return []

        return KplerProductScraper.get_parsed_infos(platform=platform, id=flow.get("id"))

    def _parse_trade(self, trade_raw, platform) -> (List[dict], List[dict], List[dict], List[dict]):
        """
        Parse a single trade and return a list of dictionaries.
        At the moment, it will only return either None or a list of single dictionary,
        but just in case one trade translates into more for us.
        :param x:
        :return:
         - trades
         - vessels
         - zones
         - products
        """

        trades = self._parse_trade_trade(trade_raw=trade_raw, platform=platform)
        # #DEBUG save trades using pickle
        # import pickle
        # # with open('trades.pickle', 'wb') as outfile:
        # #     pickle.dump(trades, outfile)
        # with open('trades.pickle', 'rb') as picklefile:
        #     trades = pickle.load(picklefile)

        vessels = self._parse_trade_vessels(vessels=get_nested(trade_raw, "vessels"))
        zones = self._parse_trade_zones(trade_raw=trade_raw)
        installations = self._parse_trade_installations(trade_raw=trade_raw)
        product_ids = set([x.get("product_id") for x in trades])
        products = [
            KplerProductScraper.get_parsed_infos(platform=platform, id=x) for x in product_ids
        ]

        return trades, vessels, zones, products, installations

    def _country_name_to_iso2(self, country_name):
        return self.cc.convert(country_name, to="ISO2") if country_name else None