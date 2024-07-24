import datetime as dt
import random
import shutil
import time
from typing import Optional
from urllib.parse import parse_qs
import pyotp
import json

from base.env import get_env
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from base.logger import logger


class KplerCredentials:
    """
    Represents the credentials required to authenticate with the Kpler API.
    """

    @staticmethod
    def from_env():
        username = get_env("KPLER_EMAIL")
        if username is None:
            raise ValueError(
                "Kpler username was not set. Is KPLER_EMAIL environment variable set correctly?"
            )
        password = get_env("KPLER_PASSWORD")
        if password is None:
            raise ValueError(
                "Kpler password was not set. Is KPLER_PASSWORD environment variable set correctly?"
            )
        otp_key = get_env("KPLER_OTP_KEY")
        if otp_key is None:
            raise ValueError(
                "Kpler OTP key was not set. Is KPLER_OTP_KEY environment variable set correctly?"
            )

        return KplerCredentials(
            username=username,
            password=password,
            otp_key=otp_key,
        )

    def __init__(self, *, username, password, otp_key):
        self.username = username
        self.password = password
        self.otp_key = otp_key

    def get_otp_value(self):
        """
        Generates the OTP value based on the key and the current time.
        """
        return pyotp.TOTP(self.otp_key).now()


class KplerToken:
    """
    Represents an authentication token and details for the Kpler API.
    """

    refresh_token_earlier = dt.timedelta(seconds=60)

    def __init__(self, *, access_token, id_token, refresh_token, expiry_time):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expiry_time = expiry_time
        self.id_token = id_token

    def should_refresh(self):
        """
        Checks if the token should be refreshed. If the token is going to expire in less
        than the refresh_token_earlier time.
        """
        return dt.datetime.now() > (self.expiry_time - KplerToken.refresh_token_earlier)


class KplerTokenManager:
    """
    Manages the authentication token for the Kpler API.

    Responsible for obtaining and refreshing the authentication token required to access the Kpler
    API. It provides a method to retrieve the token, ensuring that it is valid and not expired.
    """

    start_url = "https://terminal.kpler.com/"
    refresh_url = "https://auth.kpler.com/oauth/token"

    def __init__(self, *, credentials: KplerCredentials):
        self._credentials: KplerCredentials = credentials
        self._token: Optional[KplerToken] = None
        self._client_id: Optional[str] = None
        self._headers: Optional[dict] = None

    def get_token(self, *, reauth: bool = False):
        """
        Retrieves the authentication token, ensuring that it is valid and not expired.

        If the token is not yet obtained, this method will perform the login process to obtain a new
        token.

        If the token is going to expire soon, it will refresh the existing token.

        :returns: An instance of the KplerToken class representing the authentication token.
        """
        if reauth:
            logger.info("Reauthenticating with Kpler.")
            self._login()
        elif self._token is None:
            logger.info("No Kpler token available, logging in.")
            self._login()
        elif self._token.should_refresh():
            logger.info("Kpler token is going to expire soon, refreshing.")
            self._refresh_token()

        return self._token

    def _login(self):

        driver = KplerTokenManager._build_web_driver()

        try:
            self._take_steps_to_login(driver)

            request_for_token = KplerTokenManager._extract_token_request(driver)

            self._headers = KplerTokenManager._extract_headers_from_request(request_for_token)
            self._client_id = KplerTokenManager._extract_client_id(request_for_token)
            self._token = KplerTokenManager._extract_token(driver, request_for_token)
        finally:
            driver.quit()

    def _refresh_token(self):
        response = requests.post(
            KplerTokenManager.refresh_url,
            headers=self._headers,
            data={
                "client_id": self._client_id,
                "grant_type": "refresh_token",
                "refresh_token": self._token.refresh_token,
            },
        )

        response_body = response.json()

        self._token = KplerToken(
            access_token=response_body["access_token"],
            id_token=response_body["id_token"],
            refresh_token=response_body["refresh_token"],
            expiry_time=dt.datetime.now() + dt.timedelta(seconds=response_body["expires_in"]),
        )

    @staticmethod
    def _build_web_driver():
        """
        Builds a new web driver instance for the Chrome browser with the required options and
        capabilities to extract the tokens from the login pages.
        """
        # Create a new web driver
        chrome_options = webdriver.ChromeOptions()
        # Needed for _extract_token_request
        chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        chrome_options.add_argument("ignore-certificate-errors")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("window-size=1024,768")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--headless")

        path = shutil.which("chromedriver")

        service = Service(path) if path else Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(options=chrome_options, service=service)
        # Needed for _build_token
        driver.execute_cdp_cmd("Network.enable", {})
        return driver

    @staticmethod
    def _extract_client_id(request_for_token):
        return parse_qs(request_for_token.get("request").get("postData"))["client_id"][0]

    @staticmethod
    def _extract_token(driver, request_for_token):

        response_for_token = driver.execute_cdp_cmd(
            "Network.getResponseBody", {"requestId": request_for_token["requestId"]}
        )
        response_body = json.loads(response_for_token["body"])

        return KplerToken(
            access_token=response_body["access_token"],
            id_token=response_body["id_token"],
            refresh_token=response_body["refresh_token"],
            expiry_time=dt.datetime.now() + dt.timedelta(seconds=response_body["expires_in"]),
        )

    @staticmethod
    def _extract_headers_from_request(request):
        return request["request"]["headers"]

    def _take_steps_to_login(self, driver: webdriver.Chrome):
        # Navigate to the login page
        driver.get(KplerTokenManager.start_url)

        wait = WebDriverWait(driver, 10)

        # Sleep for a random time to avoid bot detection
        random_wait = lambda: time.sleep(random.uniform(0.5, 1.5))

        # Wait for element with class login to appear
        wait.until(EC.presence_of_element_located((By.NAME, "username")))

        # Put the username and password in the form
        driver.find_element(By.NAME, "username").send_keys(self._credentials.username)
        driver.find_element(By.NAME, "password").send_keys(self._credentials.password)
        random_wait()
        # Click the login button
        driver.find_element(By.CSS_SELECTOR, '[data-action-button-primary="true"]').click()

        # Wait for the page to load
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".map-search-input")))

    @staticmethod
    def _extract_token_request(
        driver: webdriver.Chrome,
    ):

        log_entries = driver.get_log("performance")
        parsed_entries = [json.loads(entry["message"]).get("message") for entry in log_entries]

        # Filter for network requests
        network_requests = [
            entry.get("params")
            for entry in parsed_entries
            if entry.get("method")
            in ["Network.requestWillBeSent", "Network.requestWillBeSentExtraInfo"]
        ]

        matches_request = lambda request: (
            request.get("request") is not None
            and request.get("request").get("url") == KplerTokenManager.refresh_url
            and request.get("request").get("method") == "POST"
        )

        request_for_token = next(x for x in network_requests if matches_request(x))

        return request_for_token
