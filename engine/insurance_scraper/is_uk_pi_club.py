from datetime import datetime
import re
from requests import Session

from base.logger import logger
from engine.insurance_scraper.insurance_scraper import InsuranceScraper
from typing import Optional
import json

class UKPiClubInsuranceScraper(InsuranceScraper):
    def __init__(self) -> None:
        self.base_path = "https://connect.thomasmiller.com/UKPI_ShipFinder_R"
        self.module_version = ""
        self.cache_id = ""
        self.search_ship_script_url = f"{self.base_path}/scripts/UKPI_ShipFinder_R.CommonWebBlocks.ShipSearchList.mvc.js"

        self.session = Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/53.0.2785.143 Safari/537.36"
            }
        )
        super().__init__()

    def get_insurance_start_date_for_ship(self, imo: str) -> datetime:
        try:
            self.setCSRFToken()
            self.setModuleVersion()
            self.setCacheId()

            self.setSearchShips(imo)

            ships = self.searchShips()

            list_ships = ships["data"]["v_ShipSearchResponse"]["ShipList"]["List"]

            if len(list_ships) > 1:
                logger.warning(f"Found more than one ship for {imo}, using first one")

            ship = list_ships[0]

            bluecards = self.getShipBlueCardData(ship["InsuredUnitID"])

            cards = bluecards["data"]["v_ShipFinderRetrieveResponse"][
                "CurrentBluecardList"
            ]["List"]

            for card in cards:
                if card["BluecardTypeDescription"].lower().find("bunker") != -1:
                    return datetime.strptime(card["PeriodFromDate"], "%d-%b-%Y")

            return None
        except Exception as e:
            logger.error(f"Failed to get insurance start date for {imo}: {e}")
            return None

    def setSearchShips(self, imo: str):
        api_version_cache_update = self.getCacheUpdateApiVersion()
        url = f"{self.base_path}/screenservices/UKPI_ShipFinder_R/CommonWebBlocks/ShipSearchList/ServiceAPICacheUpdate"
        data = {
            "versionInfo": {
                "moduleVersion": self.module_version,
                "apiVersion": api_version_cache_update,
            },
            "viewName": "Public.PublicShipFinderSearch",
            "inputParameters": {
                "Id": self.cache_id,
                "JSON": json.dumps({"StartIndex": 0, "Search": imo}),
            },
        }

        response = self.session.post(url, verify=False, json=data)

        if response.status_code != 200:
            raise Exception(
                f"Failed to set search ships: received {response.status_code} from server: {response.text}"
            )

    def searchShips(self):
        api_version = self.getShipSearchApiVersion()
        data = {
            "versionInfo": {
                "moduleVersion": self.module_version,
                "apiVersion": api_version,
            },
            "viewName": "Public.PublicShipFinderSearch",
            "screenData": {
                "variables": {
                    "IsFirstLoadComplete": "true",
                    "SearchCacheID": self.cache_id,
                    "_searchCacheIDInDataFetchStatus": 1,
                }
            },
        }

        url = f"{self.base_path}/screenservices/UKPI_ShipFinder_R/CommonWebBlocks/ShipSearchList/DataActionSearchForShips"
        request = self.session.post(url, json=data, verify=False)

        if request.status_code != 200:
            raise Exception(
                f"Failed to search ships: received {request.status_code} from server: {request.text}"
            )

        return request.json()

    def getShipBlueCardData(self, insured_unit_id: str) -> Optional[dict]:
        api_version = self.getShipBlueCardApiVersion()
        data = {
            "versionInfo": {
                "moduleVersion": self.module_version,
                "apiVersion": api_version,
            },
            "viewName": "Public.PublicShipFinderBluecards",
            "screenData": {
                "variables": {
                    "InsuredUnitID": insured_unit_id,
                    "_insuredUnitIDInDataFetchStatus": 1,
                }
            },
        }

        url = f"{self.base_path}/screenservices/UKPI_ShipFinder_R/CommonWebBlocks/BluecardList/DataActionShipRetrieve"

        request = self.session.post(url, json=data, verify=False)

        if request.status_code != 200:
            raise Exception(
                f"Failed to get blue card data: received {request.status_code} from server: {request.text}"
            )

        return request.json()

    def getCacheApiVersion(self) -> Optional[str]:
        """
        This script includes the required API version needed for requesting ship data.
        Currently, it is the first script within the head element.
        Example script tag:
        <script type="text/javascript" src="scripts/UKPI_ShipFinder_R.controller.js?0lB1o5DauDMgjkIHAvx+TA"></script>
        """
        url = f"{self.base_path}/scripts/UKPI_ShipFinder_R.Public.PublicShipFinderEntry.mvc.js"
        script_response = self.session.get(url, verify=False)

        if script_response.status_code != 200:
            raise Exception(
                "Failed to get api version for cache id: received {script_response.status_code} from server: {script_response.text}"
            )

        pattern = r'controller\.callServerAction\("CacheCreate",\s*"screenservices/UKPI_ShipFinder_R/Public/PublicShipFinderEntry/ServiceAPICacheCreate",\s*"([^"]+)"'

        match = re.search(pattern, script_response.text)

        if not match:
            raise Exception(
                "Failed to get api version for cache id: unable to find API version"
            )

        return match.group(1)

    def getCacheUpdateApiVersion(self) -> Optional[str]:
        script_response = self.session.get(self.search_ship_script_url, verify=False)

        if script_response.status_code != 200:
            raise Exception(
                "Failed to get api version for search ship cache: received {script_response.status_code} from server: {script_response.text}"
            )

        pattern = r'controller\.callServerAction\("CacheUpdate",\s*"screenservices/UKPI_ShipFinder_R/CommonWebBlocks/ShipSearchList/ServiceAPICacheUpdate",\s*"([^"]+)"'

        match = re.search(pattern, script_response.text)

        if not match:
            raise Exception(
                "Failed to get api version for search ship cache: unable to find API version"
            )

        return match.group(1)

    def getShipSearchApiVersion(self) -> Optional[str]:
        script_response = self.session.get(self.search_ship_script_url, verify=False)

        if script_response.status_code != 200:
            raise Exception(
                f"Failed to get api version for search ship: received {script_response.status_code} from server: {script_response.text}"
            )

        pattern = r'controller\.callDataAction\("DataActionSearchForShips",\s*"screenservices/UKPI_ShipFinder_R/CommonWebBlocks/ShipSearchList/DataActionSearchForShips",\s*"([^"]+)"'

        match = re.search(pattern, script_response.text)

        if not match:
            raise Exception(
                "Failed to get api version for search ship: unable to find API version"
            )

        return match.group(1)

    def getShipBlueCardApiVersion(self) -> Optional[str]:
        url = f"{self.base_path}/scripts/UKPI_ShipFinder_R.CommonWebBlocks.BluecardList.mvc.js"
        script_response = self.session.get(url, verify=False)

        if script_response.status_code != 200:
            raise Exception(
                f"Failed to get api version for search ship: received {script_response.status_code} from server: {script_response.text}"
            )

        pattern = r'controller\.callDataAction\("DataActionShipRetrieve",\s*"screenservices/UKPI_ShipFinder_R/CommonWebBlocks/BluecardList/DataActionShipRetrieve",\s*"([^"]+)"'

        match = re.search(pattern, script_response.text)

        if not match:
            raise Exception(
                "Failed to get api version for get ship blue card: unable to find API version"
            )

        return match.group(1)

    def setModuleVersion(self) -> Optional[str]:
        url = f"{self.base_path}/moduleservices/moduleversioninfo"
        module_response = self.session.get(url, verify=False)

        if module_response.status_code != 200:
            raise Exception(
                f"Failed to get module version: received {module_response.status_code} from server: {module_response.text}"
            )

        data = module_response.json()

        self.module_version = data["versionToken"]

    def setCacheId(self) -> Optional[str]:
        api_version = self.getCacheApiVersion()
        data = {
            "versionInfo": {
                "moduleVersion": self.module_version,
                "apiVersion": api_version,
            },
            "viewName": "Public.PublicShipFinderEntry",
            "inputParameters": {},
        }

        url = f"{self.base_path}/screenservices/UKPI_ShipFinder_R/Public/PublicShipFinderEntry/ServiceAPICacheCreate"
        request = self.session.post(url, json=data, verify=False)

        res = request.json()

        if request.status_code != 200:
            raise Exception(f"Failed to get cache Id from server: {res.text}")

        self.cache_id = res["data"]["Id"]

    def setCSRFToken(self) -> Optional[str]:
        url = f"{self.base_path}/scripts/OutSystems.js"
        response = self.session.get(url, verify=False)

        if response.status_code != 200:
            raise Exception(
                f"Failed to get csrf token: received {response.status_code} from server: {response.text}"
            )

        pattern = r'e\.AnonymousCSRFToken\s*=\s*"([^"]+)"'

        match = re.search(pattern, response.text)

        if not match:
            raise Exception("Failed to get csrf token: unable to find token")

        self.session.headers.update({"X-Csrftoken": match.group(1)})
