from abc import ABC, abstractmethod
import json
from poplib import POP3, POP3_SSL
import random
import shutil
import string
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


class DetailsGenerator:
    def generate_name(self):
        first_names = ["John", "Jane", "Alex", "Emily", "Chris", "Katie", "Michael", "Sarah"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
        return f"{random.choice(first_names)} {random.choice(last_names)}"

    # Function to generate a random address line
    def generate_address_line(self):
        street_names = ["Main St", "High St", "Park Ave", "Oak St", "Maple St", "Cedar Ave"]
        street_number = random.randint(1, 9999)
        return f"{street_number} {random.choice(street_names)}"

    # Function to generate a random city
    def generate_city(self):
        cities = [
            "New York",
            "Los Angeles",
            "Chicago",
            "Houston",
            "Phoenix",
            "Philadelphia",
            "San Antonio",
        ]
        return random.choice(cities)

    # Function to generate a random postal code
    def generate_post_code(self):
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=5))


class EquasisWebsiteAccountDriver:
    @staticmethod
    def create():
        driver = EquasisWebsiteAccountDriver._build_web_driver()
        client = AzCaptchaSolverClient(AZCAPTCHA_API_KEY)
        details_generator = DetailsGenerator()
        return EquasisWebsiteAccountDriver(driver, client, details_generator)

    @staticmethod
    def _build_web_driver():
        temp_dir = tempfile.mkdtemp()

        log_output_location = f"{temp_dir}/chromedriver.log"

        try:
            user_agent = agent_generator.random

            options = webdriver.ChromeOptions()
            options.add_argument("ignore-certificate-errors")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-gpu")
            options.add_argument("window-size=1024,768")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument(f"--user-agent={user_agent}")
            options.add_argument(f"--headless")

            existing_path = shutil.which("chromedriver")

            path = existing_path or ChromeDriverManager().install()

            service = Service(path, log_output=log_output_location)

            driver = webdriver.Chrome(service=service, options=options)

            return driver
        except Exception as e:
            logger.warn("Failed to create web driver. Outputting chromedriver.log.")
            try:
                with open(log_output_location, "r") as log_file:
                    logger.info(f"chromedriver log contents:\n{log_file.read()}")
            except Exception:
                logger.warn("Failed to read chrome driver log file", exc_info=True)
            raise e
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def __init__(
        self,
        driver: webdriver.Chrome,
        captcha_solver: AzCaptchaSolverClient,
        details_generator: DetailsGenerator,
    ):
        self.driver = driver
        self.captcha_solver = captcha_solver
        self.details_generator = details_generator

    def create_account(self, username, password):
        logger.info(f"Creating account on Equasis website: {username}")

        self._navigate_to_account_page()
        # The extension solves the captcha automatically as we navigate to the page
        self._solve_recaptcha()
        self._fill_account_info(username, password)
        self._add_personal_info()
        self._submit()

        return username

    def forget_password(self, username):
        logger.info(f"Forgetting password for Equasis account: {username}")

        self.driver.get("https://www.equasis.org/EquasisWeb/public/LostPassword")
        self.driver.find_element(By.CSS_SELECTOR, ".input_email").send_keys(username)
        self.driver.find_element(By.NAME, "Submit").click()

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
        self.driver.find_element(By.CSS_SELECTOR, "#field-firstname").send_keys(
            self.details_generator.generate_name()
        )
        self.driver.find_element(By.CSS_SELECTOR, "#field-name").send_keys(
            self.details_generator.generate_name()
        )
        self.driver.find_element(By.CSS_SELECTOR, "#field-adress").send_keys(
            self.details_generator.generate_address_line()
        )
        self.driver.find_element(By.CSS_SELECTOR, "#field-city").send_keys(
            self.details_generator.generate_city()
        )
        self.driver.find_element(By.CSS_SELECTOR, "#field-postcode").send_keys(
            self.details_generator.generate_post_code()
        )

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

        client: POP3 | POP3_SSL = self.generate_client()

        all_emails: list[list[str]] = self.consume_all_emails(client)

        client.quit()

        for email_content in all_emails:
            if self.email_matches(username, email_content):
                return self.extract_verification_details(email_content)
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

    def extract_verification_details(self, email_content: list[str]):
        link = self.extract_verification_link(email_content)
        password = self.extract_password(email_content)
        return (link, password)

    def extract_verification_link(self, email_content: list[str]):
        for line in email_content:
            if line.startswith("https://www.equasis.org/EquasisWeb/public/Activation?"):
                return line
        return None

    def extract_password(self, email_content: list[str]):
        for line in email_content:
            if line.startswith("Password : "):
                return line.split(" : ")[1].strip()
        return None


class EquasisAccount:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def __str__(self):
        return f"EquasisAccount(username={self.username}, password={self.password})"

    def __repr__(self):
        return self.__str__()


class Alias:
    def __init__(self, *, alias: str, id: str):
        self.alias = alias
        self.id = id


class EmailManager(ABC):
    @abstractmethod
    def create_email(self) -> Alias:
        pass

    @abstractmethod
    def delete_email(self, alias: Alias):
        pass


class SimpleLoginEmailManager(EmailManager):
    @staticmethod
    def from_env():
        return SimpleLoginEmailManager(simple_login_api_key=config("SIMPLE_LOGIN_API_KEY"))

    BASE_URL = "https://app.simplelogin.io/api"

    def __init__(self, *, simple_login_api_key: str):
        self.simple_login_api_key = simple_login_api_key
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authentication": f"{simple_login_api_key}",
            }
        )

    def create_email(self) -> Alias:
        response = self.session.post(f"{SimpleLoginEmailManager.BASE_URL}/alias/random/new")

        response.raise_for_status()

        content = response.json()

        return Alias(
            alias=content["email"],
            id=content["id"],
        )

    def delete_email(
        self,
        alias: Alias,
    ):
        response = self.session.delete(f"{SimpleLoginEmailManager.BASE_URL}/aliases/{alias.id}")

        response.raise_for_status()

        content = response.json()
        if not content["deleted"]:
            raise RuntimeError(f"Failed to delete alias {alias}")

        return


class GmailEmailManager(EmailManager):
    def __init__(self, *, username_pattern: str):
        self._username_pattern = username_pattern

    def create_email(self) -> Alias:
        username = self._username_pattern % str(uuid4())
        return Alias(alias=username, id=username)

    def delete_email(self, alias: Alias):
        pass


class PasswordGenerator:
    def __init__(self, length: int = 12):
        self.length = length

    def generate_password(self):
        # Define the character sets
        lower = string.ascii_lowercase
        upper = string.ascii_uppercase
        digits = string.digits
        special_chars = string.punctuation

        # Combine all character sets
        all_chars = lower + upper + digits + special_chars

        # Ensure the password contains at least one character from each set
        password = [
            random.choice(lower),
            random.choice(upper),
            random.choice(digits),
            random.choice(special_chars),
        ]

        # Fill the rest of the password length with random choices from all_chars
        password += random.choices(all_chars, k=self.length - 4)

        # Shuffle the password to prevent predictable patterns
        random.shuffle(password)

        # Convert list to string
        return "".join(password)


class EquasisAccountCreator:
    def __init__(
        self,
        *,
        client: EquasisEmailClient,
        driver: EquasisWebsiteAccountDriver,
        email_alias_manager: EmailManager,
        password_generator: PasswordGenerator,
    ):
        self.email_client = client
        self.website_driver = driver
        self.email_alias_manager = email_alias_manager
        self.password_generator = password_generator

    def create_account(self):
        alias = self.email_alias_manager.create_email()
        username = alias.alias
        password = self.password_generator.generate_password()

        try:
            # We start by creating the account
            self.website_driver.create_account(username, password)
            link, _ = self.email_client.get_verification_link_from_emails(username)
            self.website_driver.verify_account(link)

            # Then to get the account to work, we have to reset the password
            self.website_driver.forget_password(username)
            link, new_password = self.email_client.get_verification_link_from_emails(username)
            self.website_driver.verify_account(link)

            if new_password is None:
                raise ValueError("Failed to get new password")
            return EquasisAccount(username, new_password)
        finally:
            self.email_alias_manager.delete_email(alias)


def default_from_env_generator(n_accounts: int) -> list[EquasisAccount]:

    equasis_driver = EquasisWebsiteAccountDriver.create()
    email_client = EquasisEmailClient()
    email_alias_manager = GmailEmailManager(username_pattern=USERNAME)
    password_generator = PasswordGenerator(12)

    account_creator = EquasisAccountCreator(
        client=email_client,
        driver=equasis_driver,
        email_alias_manager=email_alias_manager,
        password_generator=password_generator,
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
