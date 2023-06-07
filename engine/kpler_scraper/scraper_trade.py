from typing import List

from engine.kpler_scraper.scraper import *
from engine.kpler_scraper.misc import get_nested


class KplerTradeScraper(KplerScraper):
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
            [trades.extend(self._parse_trade(x) or []) for x in trades_raw]

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

    def _parse_sts(self, x, origin_or_destination):
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

    def _parse_trade(self, x) -> (List[dict], List[dict]):
        """
        Parse a single trade and return a list of dictionaries.
        At the moment, it will only return either None or a list of single dictionary,
        but just in case one trade translates into more for us.
        Also return a list of StS.
        :param x:
        """
        trade = {}
        # General
        trade["id"] = x.get("id")
        status_dict = {
            "In Transit": base.ONGOING,
            "Delivered": base.COMPLETED,
        }
        if x.get("status") not in status_dict:
            return None
        else:
            trade["status"] = status_dict[x.get("status")]

        trade["departure_date_utc"] = pd.to_datetime(x.get("start"))
        trade["arrival_date_utc"] = pd.to_datetime(x.get("end"))

        # Berth
        trade["departure_berth_name"] = x.get("berth", {}).get("name")
        trade["departure_berth_id"] = x.get("berth", {}).get("id")

        # Flows
        flows = x.get("flowQuantities")
        if len(flows) > 1:
            logger.warning(f"More than one flow in trade {x.get('id')}. Not managed yet.")
            return None
        elif len(flows) == 0:
            return None

        flow = flows[0]
        trade["product_id"] = flow.get("confirmedProduct").get("productId")
        trade["product_name"] = flow.get("confirmedProduct").get("name")
        trade["product_type"] = flow.get("confirmedProduct").get("type")

        # Origin and destination
        trade["from_installation_id"] = get_nested(x, "portCallOrigin", "installation", "id")
        trade["from_port_id"] = get_nested(x, "portCallOrigin", "installation", "port", "id")
        trade["has_sts"] = False
        if get_nested(x, "portCallOrigin", "shipToShip"):
            trade["from_sts"] = self._parse_sts(x, origin_or_destination="origin")
            trade["has_sts"] = True
        else:
            trade["from_sts"] = None

        if x.get("portCallDestination"):
            trade["to_installation_id"] = get_nested(x, "portCallDestination", "installation", "id")
            trade["to_port_id"] = get_nested(x, "portCallDestination", "installation", "port", "id")
        else:
            trade["to_installation_id"] = None
            trade["to_port_id"] = None

        if get_nested(x, "portDestinationOrigin", "shipToShip"):
            trade["to_sts"] = self._parse_sts(x, origin_or_destination="destination")
            trade["has_sts"] = True
        else:
            trade["to_sts"] = None

        # Vessels
        vessels = x.get("vessels")
        if len(vessels) > 1 and not trade["has_sts"]:
            logger.warning(f"More than one vessel in trade {x.get('id')}. Not managed yet.")
            return None
        elif len(vessels) > 2 and trade["has_sts"]:
            logger.warning(f"More than two vessels in sts trade {x.get('id')}. Not managed yet.")
            return None
        elif len(vessels) == 0:
            logger.warning(f"No vessel in trade {x.get('id')}. Not managed yet.")
            return None

        if len(vessels) == 1:
            vessel = vessels[0]
            trade["vessel_id"] = vessel.get("id")
            trade["vessel_name"] = vessel.get("name")
            trade["vessel_imo"] = vessel.get("imo")

        elif len(vessels) == 2 and trade["has_sts"]:
            vessel_from = vessels[0]
            vessel_to = vessels[1]
            trade["vessel_id"] = vessel_from.get("id")
            trade["vessel_name"] = vessel_from.get("name")
            trade["vessel_imo"] = vessel_from.get("imo")
            trade["vessel_from_id"] = vessel_from.get("id")
            trade["vessel_from_name"] = vessel_from.get("name")
            trade["vessel_from_imo"] = vessel_from.get("imo")
            trade["vessel_to_id"] = vessel_to.get("id")
            trade["vessel_to_name"] = vessel_to.get("name")
            trade["vessel_to_imo"] = vessel_to.get("imo")
        else:
            logger.warning(f"Unmanaged number of vessels for rrade {x.get('id')}")
            return None

        return [trade]
