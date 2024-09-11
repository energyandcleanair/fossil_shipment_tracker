from abc import ABC, abstractmethod
import json
from poplib import POP3, POP3_SSL
import random
import shutil
import string
import sys
import tempfile
from time import sleep
from typing import Union
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

from engines.company_scraper.accounts_details import DetailsGenerator
from engines.company_scraper.accounts_email_generators import EmailManager
from engines.company_scraper.accounts_email_reader import EquasisEmailClient

from .accounts_captcha import AzCaptchaSolverClient, SolutionError

agent_generator = UserAgent()

REGISTRATION_URL = (
    "https://www.equasis.org/EquasisWeb/public/Registration?fs=ConditionsRegistration"
)
RECAPTCHA_IFRAME_SELECTOR = "iframe[title=reCAPTCHA]"
RECAPTCHA_WRAPPER_SELECTOR = ".g-recaptcha"
RECAPTCHA_RESPONSE_SELECTOR = "#g-recaptcha-response"

AZCAPTCHA_API_KEY = config("AZCAPTCHA_API_KEY")


class EquasisWebsiteAccountDriver:
    @staticmethod
    def create_from_env():
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
