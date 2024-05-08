import json
from poplib import POP3, POP3_SSL
import shutil
import sys
import tempfile
from time import sleep
import zipfile

import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import Select

from decouple import config
from fake_useragent import UserAgent
from uuid import uuid4

from datetime import datetime

from base.logger import logger

from webdriver_manager.chrome import ChromeDriverManager

agent_generator = UserAgent()

REGISTRATION_URL = (
    "https://www.equasis.org/EquasisWeb/public/Registration?fs=ConditionsRegistration"
)
RECAPTCHA_IFRAME_SELECTOR = "iframe[title=reCAPTCHA]"
RECAPTCHA_WRAPPER_SELECTOR = ".g-recaptcha"
RECAPTCHA_RESPONSE_SELECTOR = "#g-recaptcha-response"

PASSWORD = config("EQUASIS_PASSWORD")
USERNAME = config("EQUASIS_USERNAME_PATTERN")

RECEIVER_EMAIL_USERNAME = config("RECEIVER_EMAIL_USERNAME")
RECEIVER_EMAIL_PASSWORD = config("RECEIVER_EMAIL_PASSWORD")
RECEIVER_EMAIL_POP_SERVER = config("RECEIVER_EMAIL_POP_SERVER")
RECEIVER_EMAIL_POP_PORT = config("RECEIVER_EMAIL_POP_PORT")
RECEIVER_EMAIL_POP_SECURE = config("RECEIVER_EMAIL_POP_SECURE", default="true")

RECEIVER_EMAIL_POP_SECURE = RECEIVER_EMAIL_POP_SECURE.lower() == "true"

AZCAPTCHA_API_KEY = config("AZCAPTCHA_API_KEY")


class AzCaptchaSolverClient:
    def __init__(
        self,
        api_key: str,
        initial_wait=20,
        backoff_interval=5,
        exponential_rate=1.5,
        timeout=5 * 60,
    ):
        self.api_key = api_key
        self.initial_wait = initial_wait
        self.backoff_interval = backoff_interval
        self.exponential_rate = exponential_rate
        self.timeout_seconds = timeout

    def solve_captcha(self, site_key: str, page_url: str):
        captcha_id = self._start_solver(site_key, page_url)

        return self._get_captcha_solution(captcha_id)

    def _start_solver(self, site_key, page_url):
        logger.info("Sending captcha to azcaptcha")
        response = requests.post(
            "https://azcaptcha.com/in.php",
            data={
                "method": "userrecaptcha",
                "googlekey": site_key,
                "key": self.api_key,
                "pageurl": page_url,
                "json": 1,
            },
        )

        if response.status_code != 200:
            raise ValueError("Failed to send captcha to azcaptcha")

        result = response.json()

        if result["status"] != 1:
            raise ValueError(f"Failed to send captcha to azcaptcha: {result['request']}")

        return result["request"]

    def _get_captcha_solution(self, captcha_id: str):
        # Stored for checking timeouts.
        start_time = datetime.now()
        # The service requests that we wait an initial period at the start.
        sleep(self.initial_wait)
        # Stored for exponential backoff.
        check_attempts = 0
        while True:
            response_data = self._check_captcha(captcha_id)

            # If the captcha has been solved, we exit
            if response_data["status"] == 1:
                return response_data["request"]

            seconds_elapsed = (datetime.now() - start_time).seconds
            # If we have been waiting too long, give up and raise an error.
            if seconds_elapsed > self.timeout_seconds:
                raise TimeoutError(
                    f"Failed to solve captcha in {self.timeout_seconds} seconds and {check_attempts} checks"
                )

            # If the solver returned an error, raise an error.
            if response_data["request"] != "CAPCHA_NOT_READY":
                response_error = response_data["request"]
                raise ValueError(f"Failed to solve captcha: {response_error}")

            # Otherwise, wait and try again
            check_attempts += 1
            sleep(self.backoff_interval * (self.exponential_rate**check_attempts))

    def _check_captcha(self, captcha_id):
        logger.info("Getting captcha solution from azcaptcha")
        response = requests.get(
            "https://azcaptcha.com/res.php",
            params={
                "key": self.api_key,
                "action": "get",
                "id": captcha_id,
                "json": 1,
            },
        )

        if response.status_code != 200:
            raise ValueError("Failed to get captcha solution from azcaptcha")

        response_data = response.json()
        return response_data


class EquasisWebsiteAccountDriver:
    @staticmethod
    def create():
        driver = EquasisWebsiteAccountDriver._build_web_driver()
        client = AzCaptchaSolverClient(AZCAPTCHA_API_KEY)
        return EquasisWebsiteAccountDriver(driver, client)

    @staticmethod
    def _build_web_driver():
        user_agent = agent_generator.random

        options = webdriver.ChromeOptions()
        options.add_argument("ignore-certificate-errors")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument(f"--user-agent={user_agent}")
        options.add_argument(f"--headless")

        path = shutil.which("chromedriver")

        if path is None:
            service = Service(ChromeDriverManager().install())
        else:
            service = Service(path)

        driver = webdriver.Chrome(service=service, options=options)

        return driver

    def __init__(self, driver: webdriver.Chrome, captcha_solver: AzCaptchaSolverClient):
        self.driver = driver
        self.captcha_solver = captcha_solver

    def create_account(self, username, password):
        logger.info(f"Creating account on Equasis website: {username}")

        self._navigate_to_account_page()
        # The extension solves the captcha automatically as we navigate to the page
        self._solve_recaptcha()
        self._fill_account_info(username, password)
        self._add_personal_info()
        self._submit()

        return username

    def _navigate_to_account_page(self):
        logger.info("Navigating to Equasis registration page")
        self.driver.switch_to.new_window("tab")
        self.driver.get(REGISTRATION_URL)
        WebDriverWait(self.driver, 10, ignored_exceptions=EC.StaleElementReferenceException).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, RECAPTCHA_IFRAME_SELECTOR))
        )

    def _submit(self):
        logger.info("Submitting registration form")
        self.driver.find_element(By.NAME, "insertUser").click()

        # Say yes to alert
        self.driver.switch_to.alert.accept()

    def _fill_account_info(self, username, password):
        logger.info("Filling account info")
        self.driver.find_element(By.CSS_SELECTOR, "#field-mail").send_keys(username)
        self.driver.find_element(By.CSS_SELECTOR, "#field-pwd").send_keys(password)
        self.driver.find_element(By.CSS_SELECTOR, "#field-confirm-pwd").send_keys(password)

    def _add_personal_info(self):
        logger.info("Filling personal info")
        self.driver.find_element(By.CSS_SELECTOR, "#field-firstname").send_keys("A")
        self.driver.find_element(By.CSS_SELECTOR, "#field-name").send_keys("B")
        self.driver.find_element(By.CSS_SELECTOR, "#field-adress").send_keys("HK")
        self.driver.find_element(By.CSS_SELECTOR, "#field-city").send_keys("Hong Kong")
        self.driver.find_element(By.CSS_SELECTOR, "#field-postcode").send_keys("0")

        # Selects
        Select(self.driver.find_element(By.CSS_SELECTOR, "select[name=p_title]")).select_by_value(
            "Mr"
        )
        Select(self.driver.find_element(By.CSS_SELECTOR, "select[name=p_country]")).select_by_value(
            "0275"
        )
        Select(
            self.driver.find_element(By.CSS_SELECTOR, "select[name=p_sector_activity]")
        ).select_by_value("013")
        Select(
            self.driver.find_element(By.CSS_SELECTOR, "select[name=p_how_equasis]")
        ).select_by_value("05")

    def _solve_recaptcha(self):
        logger.info("Solving recaptcha")

        site_key = self.driver.find_element(
            By.CSS_SELECTOR, RECAPTCHA_WRAPPER_SELECTOR
        ).get_attribute("data-sitekey")

        captcha_result = self.captcha_solver.solve_captcha(site_key, self.driver.current_url)

        self.driver.execute_script(
            f"document.querySelector('{RECAPTCHA_RESPONSE_SELECTOR}').innerHTML = '{captcha_result}';"
        )

    def verify_account(self, verification_link: str):
        logger.info("Verifying account")
        self.driver.get(verification_link)

    def close(self):
        logger.info("Closing Equasis website driver")
        self.driver.quit()


class EquasisEmailClient:
    @staticmethod
    def create_pop_client():
        pop_client = (
            POP3_SSL(RECEIVER_EMAIL_POP_SERVER, RECEIVER_EMAIL_POP_PORT, timeout=5 * 60)
            if RECEIVER_EMAIL_POP_SECURE
            else POP3(RECEIVER_EMAIL_POP_SERVER, RECEIVER_EMAIL_POP_PORT, timeout=5 * 60)
        )

        pop_client.user(RECEIVER_EMAIL_USERNAME)
        pop_client.pass_(RECEIVER_EMAIL_PASSWORD)
        return pop_client

    def __init__(self, client_generator=create_pop_client):
        self.generate_client = client_generator

    def get_verification_link_from_emails(self, username):
        logger.info(f"Waiting for email verification for {username}")
        start_time = datetime.now()
        max_wait = 2 * 60
        while True:
            sleep(5)

            verification_link = self.find_verification_link_in_mailbox(username)

            if verification_link != None:
                break

            if (datetime.now() - start_time).seconds > max_wait:
                raise TimeoutError("Email verification timeout")
        return verification_link

    def find_verification_link_in_mailbox(self, username: str):
        logger.info("Reading emails")

        client = self.generate_client()

        all_emails: list[list[str]] = self.consume_all_emails(client)

        for email_content in all_emails:
            if self.email_matches(username, email_content):
                return self.extract_verification_link(email_content)
        return None

    def consume_all_emails(self, client: POP3 | POP3_SSL):
        num_messages, _ = client.stat()
        all_emails = []
        for i in range(num_messages):
            email_index = i + 1
            all_emails.append(self.read_email_content(client, email_index))
            client.dele(email_index)
        return all_emails

    def read_email_content(self, client: POP3 | POP3_SSL, email_index: int):
        encoded_lines = client.retr(email_index)[1]
        lines = [line.decode("utf-8") for line in encoded_lines]
        return lines

    def delete_remaining_emails(self, pop_client):
        num_messages, _ = pop_client.stat()
        for i in range(num_messages):
            pop_client.dele(i + 1)

    def email_matches(self, username: str, email_content: list[str]):
        for line in email_content:
            if line.startswith("To: ") and username in line:
                return True
        return False

    def extract_verification_link(self, email_content: list[str]):
        for line in email_content:
            if line.startswith("https://www.equasis.org/EquasisWeb/public/Activation?"):
                return line
        raise ValueError("No verification link found in email")


class EquasisAccount:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def __str__(self):
        return f"EquasisAccount(username={self.username}, password={self.password})"

    def __repr__(self):
        return self.__str__()


class EquasisAccountCreator:
    def __init__(
        self,
        *,
        client: EquasisEmailClient,
        driver: EquasisWebsiteAccountDriver,
        username_pattern: str,
        password: str,
    ):
        self.email_client = client
        self.website_driver = driver

        self.username_pattern = username_pattern
        self.password = password

    def create_account(self):
        uuid = str(uuid4()).replace("-", "")
        username = self.username_pattern % uuid

        username = self.website_driver.create_account(username, self.password)
        link = self.email_client.get_verification_link_from_emails(username)
        self.website_driver.verify_account(link)
        return EquasisAccount(username, self.password)


def default_from_env_generator(n_accounts: int) -> list[EquasisAccount]:

    equasis_driver = EquasisWebsiteAccountDriver.create()
    email_client = EquasisEmailClient()

    account_creator = EquasisAccountCreator(
        client=email_client, driver=equasis_driver, username_pattern=USERNAME, password=PASSWORD
    )

    accounts = []

    max_tries = 3

    for _ in range(n_accounts):
        for attempt in range(max_tries):
            try:
                account = account_creator.create_account()
                accounts.append(account)
                break
            except Exception:
                logger.info(
                    f"Failed to create account: attempt {attempt+1}/{max_tries}",
                    stack_info=True,
                    exc_info=True,
                )
                pass

    equasis_driver.close()

    return accounts
