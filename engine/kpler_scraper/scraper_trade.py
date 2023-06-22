from typing import List, Dict, Any

from engine.kpler_scraper.scraper import *
from engine.kpler_scraper.scraper_product import KplerProductScraper
from engine.kpler_scraper.misc import get_nested


class KplerTradeScraper(KplerScraper):
    def __init__(self):
        super().__init__()

    def get_trades(
        self, platform, from_iso2=None, to_zone=None, date_from=-30, product=None, sts_only=False
    ):

        if sts_only:
            operational_filter = "shipToShip"
        else:
            operational_filter = None

        for current_iso2 in to_list(from_iso2):
            date_from = to_datetime(date_from)
            from_zone = self.get_zone_dict(iso2=current_iso2, platform=platform)
            trades_raw = []

            query_from = 0
            while True:
                print("Querying")
                size, query_trades_raw = self.get_trades_raw(
                    from_zone=from_zone,
                    platform="liquids",
                    query_from=query_from,
                    operational_filter=operational_filter,
                )
                trades_raw.extend(query_trades_raw)
                query_from += size
                if (
                    size == 0
                    or min([pd.to_datetime(x.get("start")) for x in query_trades_raw]) < date_from
                ):
                    break

            trades = []
            vessels = []
            zones = []
            products = []

            for x in trades_raw:
                trades_, vessels_, zones_, products_ = self._parse_trade(x, platform=platform)
                trades.extend(trades_)
                vessels.extend(vessels_)
                zones.extend(zones_)
                products.extend(products_)

            trades_df = pd.DataFrame(trades)
            vessels_df = pd.DataFrame(vessels)
            zones_df = pd.DataFrame(zones)
            products_df = pd.DataFrame(products)

        return trades

    def get_trades_raw(
        self,
        platform,
        from_zone=None,
        to_zone=None,
        query_from=0,
        product=None,
        operational_filter=None,
    ):

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

        from_locations = [x["resourceType"][0].lower() + str(x["id"]) for x in from_locations]
        to_locations = (
            [self.get_zone_dict(id=to_zone.get("id"), name=to_zone.get("name"), platform=platform)]
            if to_zone
            else []
        )
        to_locations = [x["resourceType"][0].lower() + str(x["id"]) for x in to_locations]
        # from: 0
        # size: 100
        # view: kpler
        # withForecasted: false
        # withFreightView: false
        # withProductEstimation: false
        # locations: z757

        # Get zone dict
        params_raw = {
            "from": query_from,
            "size": 1000,
            "view": "kpler",
            "withForecasted": "false",
            "withFreightView": "false",
            "withProductEstimation": "false",
            "locations": from_locations,
            "operationalFilter": operational_filter,
        }

        if product is not None:
            params_raw["variables"]["where"]["productIds"] = [
                self.get_product_id(platform=platform, name=product)
            ]

        token = self.token  # get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/trades",
            "liquids": "https://terminal.kpler.com/api/trades",
            "lng": "https://lng.kpler.com/api/trades",
        }.get(platform)

        headers = {
            "Authorization": f"Bearer {token}",
            "x-web-application-version": "v21.316.0",
            "content-type": "application/json",
        }
        try:
            r = requests.get(url, params=params_raw, headers=headers)
        except requests.exceptions.ChunkedEncodingError:
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        trades_raw = r.json()
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

    def _parse_trade_vessels(self, vessels) -> (List[dict]):

        return [
            {
                "id": x.get("id"),
                "name": x.get("name"),
                "imo": x.get("imo"),
                "mmsi": x.get("mmsi"),
            }
            for x in vessels
        ]

    def _parse_trade_trade(self, trade_raw) -> (List[dict]):

        trade = {}
        # General
        trade["id"] = trade_raw.get("id")
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

        # Berth
        trade["departure_berth_name"] = trade_raw.get("berth", {}).get("name")
        trade["departure_berth_id"] = trade_raw.get("berth", {}).get("id")

        # Flows
        flows = trade_raw.get("flowQuantities")
        if len(flows) > 1:
            logger.warning(f"More than one flow in trade {x.get('id')}. Not managed yet.")
            return []
        elif len(flows) == 0:
            return []

        flow = flows[0]
        trade["product_id"] = flow.get("confirmedProduct").get("productId")
        trade["product_name"] = flow.get("confirmedProduct").get("name")
        trade["product_type"] = flow.get("confirmedProduct").get("type")

        # Origin and destination
        trade["from_installation_id"] = get_nested(
            trade_raw, "portCallOrigin", "installation", "id"
        )
        trade["from_zone_id"] = get_nested(trade_raw, "portCallOrigin", "zone", "id")
        trade["from_port_id"] = get_nested(
            trade_raw, "portCallOrigin", "installation", "port", "id"
        )
        trade["from_sts"] = get_nested(trade_raw, "portCallOrigin", "shipToShip")

        trade["to_installation_id"] = get_nested(
            trade_raw, "portCallDestination", "installation", "id"
        )
        trade["to_zone_id"] = get_nested(trade_raw, "portCallDestination", "zone", "id")
        trade["to_port_id"] = get_nested(
            trade_raw, "portCallDestination", "installation", "port", "id"
        )
        trade["to_sts"] = get_nested(trade_raw, "portCallDestination", "shipToShip")

        # Vessels
        trade["vessel_ids"] = [y.get("id") for y in trade_raw.get("vessels")]
        return [trade]

    def _parse_trade_flows(self, trade_raw) -> (List[dict]):

        trade_id = trade_raw.get("id")
        flows_raw = trade_raw.get("flowQuantities")
        if len(flows_raw) == 0:
            return []

        flows = []
        for flow_raw in flows_raw:

            flow = {}
            flow["trade_id"] = trade_id
            flow["product_id"] = flow_raw.get("confirmedProduct").get("productId")
            # flow["product_name"] = flow_raw.get("confirmedProduct").get("name")
            # flow["product_type"] = flow_raw.get("confirmedProduct").get("type")
            flow["mass"] = flow_raw.get("flowQuantity").get("mass")
            flow["volume"] = flow_raw.get("flowQuantity").get("volume")
            flow["energy"] = flow_raw.get("flowQuantity").get("energy")
            flow["volume_gas"] = flow_raw.get("flowQuantity").get("volume_gas")
            flows += [flow]

        return flows

    def _parse_trade_zone(self, zone) -> (List[dict]):

        if not zone:
            return []

        primary_zone = {}
        primary_zone["id"] = zone.get("id")
        primary_zone["name"] = zone.get("name")
        primary_zone["type"] = zone.get("type")

        primary_zone["port_id"] = (zone.get("port") or {}).get("id")
        primary_zone["port_name"] = (zone.get("port") or {}).get("name")
        primary_zone["country_id"] = (zone.get("country") or {}).get("id")
        primary_zone["country_name"] = (zone.get("country") or {}).get("name")
        primary_zone["country_iso2"] = (
            self.cc.convert(primary_zone["country_name"], to="ISO2")
            if primary_zone["country_name"]
            else None
        )

        port = zone.get("port") or {}
        port_zone = {}
        port_zone["id"] = port.get("id")
        port_zone["name"] = port.get("name")
        port_zone["type"] = port.get("type")
        port_zone["country_id"] = primary_zone["country_id"]
        port_zone["country_name"] = primary_zone["country_name"]
        port_zone["country_iso2"] = primary_zone["country_iso2"]

        return [primary_zone, port_zone]

    def _parse_trade_products(self, flow, platform) -> (List[dict]):

        if not flow:
            return []

        # commodity = flow.get("closestAncestorCommodity") or {}
        # grade = flow.get("closestAncestorGrade") or {}
        # group = flow.get("closestAncestorGroup") or {}
        # family = flow.get("closestAncestorFamily") or {}

        primary_product = {}
        # primary_product["id"] = flow.get("id")
        return KplerProductScraper.get_infos(platform=platform, id=flow.get("id"))

        # primary_product["name"] = flow.get("name")
        # primary_product["full_name"] = flow.get("fullName")
        # primary_product["type"] = flow.get("type")
        # primary_product["grade_id"] = grade.get("id")
        # primary_product["grade_name"] = grade.get("name")
        # primary_product["commodity_id"] = commodity.get("id")
        # primary_product["commodity_name"] = commodity.get("name")
        # primary_product["group_id"] = group.get("id")
        # primary_product["group_name"] = group.get("name")
        # primary_product["family_id"] = family.get("id")
        # primary_product["family_name"] = family.get("name")

        # grade_product = {}
        # grade_product["id"] = grade.get("id")
        # grade_product["name"] = grade.get("name")
        # grade_product["full_name"] = grade.get("fullName")
        # grade_product["type"] = grade.get("type")
        # grade_product["grade_id"] = grade.get("id")
        # grade_product["grade_name"] = grade.get("name")
        # grade_product["commodity_id"] = commodity.get("id")
        # grade_product["commodity_name"] = commodity.get("name")
        # grade_product["group_id"] = group.get("id")
        # grade_product["group_name"] = group.get("name")
        # grade_product["family_id"] = family.get("id")
        # grade_product["family_name"] = family.get("name")
        #
        # commodity_product = {}
        # commodity_product["id"] = commodity.get("id")
        # commodity_product["name"] = commodity.get("name")
        # commodity_product["full_name"] = commodity.get("fullName")
        # commodity_product["type"] = commodity.get("type")
        # commodity_product["grade_id"] = None
        # commodity_product["grade_name"] = None
        # commodity_product["commodity_id"] = commodity.get("id")
        # commodity_product["commodity_name"] = commodity.get("name")
        # commodity_product["group_id"] = group.get("id")
        # commodity_product["group_name"] = group.get("name")
        # commodity_product["family_id"] = family.get("id")
        # commodity_product["family_name"] = family.get("name")
        #
        # group_product = {}
        # group_product["id"] = group.get("id")
        # group_product["name"] = group.get("name")
        # group_product["full_name"] = group.get("fullName")
        # group_product["type"] = group.get("type")
        # group_product["grade_id"] = None
        # group_product["grade_name"] = None
        # group_product["commodity_id"] = None
        # group_product["commodity_name"] = None
        # group_product["group_id"] = group.get("id")
        # group_product["group_name"] = group.get("name")
        # group_product["family_id"] = family.get("id")
        # group_product["family_name"] = family.get("name")
        #
        # family_product = {}
        # family_product["id"] = family.get("id")
        # family_product["name"] = family.get("name")
        # family_product["full_name"] = family.get("fullName")
        # family_product["type"] = family.get("type")
        # family_product["grade_id"] = None
        # family_product["grade_name"] = None
        # family_product["commodity_id"] = None
        # family_product["commodity_name"] = None
        # family_product["group_id"] = None
        # family_product["group_name"] = None
        # family_product["family_id"] = family.get("id")
        # family_product["family_name"] = family.get("name")
        #
        # return [primary_product, grade_product, commodity_product, group_product]

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

        trades = self._parse_trade_trade(trade_raw=trade_raw)
        # #DEBUG save trades using pickle
        # import pickle
        # # with open('trades.pickle', 'wb') as outfile:
        # #     pickle.dump(trades, outfile)
        # with open('trades.pickle', 'rb') as picklefile:
        #     trades = pickle.load(picklefile)

        vessels = self._parse_trade_vessels(vessels=get_nested(trade_raw, "vessels"))
        from_zones = self._parse_trade_zone(zone=get_nested(trade_raw, "portCallOrigin", "zone"))
        to_zones = self._parse_trade_zone(zone=get_nested(trade_raw, "portCallDestination", "zone"))
        zones = from_zones + to_zones

        products = [
            self._parse_trade_products(flow=x, platform=platform)
            for x in get_nested(trade_raw, "flowQuantities")
        ]
        # sts = self._parse_trade_sts(trade_raw)
        return trades, vessels, zones, products
