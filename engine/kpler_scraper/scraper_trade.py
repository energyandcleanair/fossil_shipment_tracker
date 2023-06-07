from typing import List

from engine.kpler_scraper.scraper import *
from engine.kpler_scraper.misc import get_nested


class KplerTradeScraper(KplerScraper):
    def get_trades_raw(
        self,
        platform,
        from_zone=None,
        to_zone=None,
        cursor_after=0,
        product=None,
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
            "from": cursor_after,
            "size": 1000,
            "view": "kpler",
            "withForecasted": "false",
            "withFreightView": "false",
            "withProductEstimation": "false",
            "locations": from_locations,
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
        trades = []
        [trades.extend(self._parse_trade(x) or []) for x in trades_raw]

        return len(trades_raw), trades

    def _parse_sts(self, x, origin_or_destination):
        sts = {}
        if origin_or_destination == "origin":
            sts_raw = get_nested(x, "portCallOrigin", "shipToShipInfo")
        elif origin_or_destination == "destination":
            sts_raw = get_nested(x, "portCallDestination", "shipToShipInfo")
        else:
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

        if get_nested(x, "portCallOrigin", "shipToShip"):
            trade["from_sts"] = self._parse_sts(x, origin_or_destination="origin")
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
        else:
            trade["to_sts"] = None

        # Vessels
        vessels = x.get("vessels")
        if len(vessels) > 1:
            logger.warning(f"More than one vessel in trade {x.get('id')}. Not managed yet.")
            return None
        elif len(vessels) == 0:
            logger.warning(f"No vessel in trade {x.get('id')}. Not managed yet.")
            return None

        vessel = vessels[0]
        trade["vessel_id"] = vessel.get("id")
        trade["ship_imo"] = vessel.get("imo")

        return [trade]
