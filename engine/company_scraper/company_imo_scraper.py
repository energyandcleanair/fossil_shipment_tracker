from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys

import re
import shutil

import pandas as pd

class CompanyImoScraper:
    """
    Class for scrapig IMO/detailed information about ship company registration and address

    """

    def __init__(self, base_url, service=None):
        self.service = service
        self.browser = None
        self.base = base_url

    def _wait_for_object(self, item, by, browser=None, wait_time=30):
        """

        Parameters
        ----------
        item : item to wait for
        by : method to use (eg. By.CSS_SELECTOR)
        browser : browser object
        wait_time : time to wait in seconds

        Returns
        -------
        Returns element if found

        """

        if not browser:
            browser = self.browser

        try:
            element = WebDriverWait(browser, wait_time).until(
                EC.presence_of_element_located((by, item))
            )
        except TimeoutException:
            print("Failed to find object...")
            return None

        if not element:
            return None

        return element

    def perform_login(self, username, password, browser=None, ntries=5):
        """
        Log into the IMO website

        Parameters
        ----------
        username : username
        password : password
        browser : browser object
        ntries : number of tries to attempt to log in

        Returns
        -------

        """

        if not browser:
            browser = self.browser

        AUTHOR_CSS = "[id$=AuthorityType][class='form-control']"
        LOGIN_FIELD_CSS = "[id$=txtUsername][class='form-control']"
        PWD_FIELD_CSS = "[id$=txtPassword][class='form-control']"
        LOGIN_BTN_CSS = "[id$=btnLogin][class='btn btn-default']"
        SRCH_BTN = "[id$=btnSearchCompanies][class='button']"

        browser.get(self.base)

        button_search = self._wait_for_object(item=AUTHOR_CSS, by=By.CSS_SELECTOR)

        if not button_search:
            return False

        author_select = Select(browser.find_element(By.CSS_SELECTOR, AUTHOR_CSS))
        author_select.select_by_visible_text("Public Users")

        username_select = WebDriverWait(
            browser, 10, ignored_exceptions=EC.StaleElementReferenceException
        ).until(EC.element_to_be_clickable((By.CSS_SELECTOR, LOGIN_FIELD_CSS)))

        # selenium doesn't support webelement refresh, so we have to retry manually

        for i in range(0, 3):
            try:
                username_select = self.browser.find_element(By.CSS_SELECTOR, LOGIN_FIELD_CSS)

                ActionChains(browser).click(username_select).send_keys(username).send_keys(
                    Keys.ENTER
                ).perform()

                login_button = WebDriverWait(
                    browser, 3, ignored_exceptions=EC.StaleElementReferenceException
                ).until(EC.element_to_be_clickable((By.CSS_SELECTOR, LOGIN_BTN_CSS)))
            except TimeoutException:
                continue
            except EC.StaleElementReferenceException:
                continue

            break

        pwd_field = self.browser.find_element(By.CSS_SELECTOR, PWD_FIELD_CSS)

        if pwd_field is None:
            return False

        ActionChains(browser).click(pwd_field).send_keys(password).send_keys(Keys.ENTER).perform()

        # verify we logged in
        search_button = WebDriverWait(
            browser, 10, ignored_exceptions=EC.StaleElementReferenceException
        ).until(EC.element_to_be_clickable((By.CSS_SELECTOR, SRCH_BTN)))

        if not search_button:
            return False

        return True

    def initialise_browser(self, options=None, browser=None, headless=False):
        """
        Initialise web browser

        Parameters
        ----------
        options : webdriver options
        browser : webdriver

        Returns
        -------

        """

        if not options:
            options = webdriver.ChromeOptions()
            options.add_argument("ignore-certificate-errors")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            if headless:
                options.add_argument("--headless")

        if not browser:

            if not self.service:
                # Use the system managed chromedriver if available
                path = shutil.which("chromedriver")

                if path is None:
                    self.service = Service(ChromeDriverManager().install())
                else:
                    self.service = Service(path)

            self.browser = webdriver.Chrome(service=self.service, options=options)
        else:
            self.browser = browser

    def get_information(self, search_text, search_by="ImoNumber", browser=None):
        """
        Returns the registration and address of selected name or imo of company

        Parameters
        ----------
        search_text : this can be the company name or imo
        search_by : whether to search by name or imo (CompanyName or ImoNumber)
        browser : browser object

        Returns
        -------
        Returns the registration, address, name, imo

        """

        if not browser:
            browser = self.browser

        table_html = self._search_data(
            search_text=search_text, search_by=search_by, browser=browser
        )

        if table_html:
            table_df = pd.read_html(table_html)[0]

            if table_df.empty:
                return None

            try:
                registration, name, imo = (
                    table_df["Registered in"].values.tolist(),
                    table_df["Name"].values.tolist(),
                    table_df["IMO Company Number"].values.tolist(),
                )

                return list(zip(registration, name, imo))

            except KeyError:
                return None

        return None

    def get_detailed_information(self, search_text, search_by="IMO"):
        """
        Find the address of selected imo/name

        Parameters
        ----------
        search_by :
        search_text : base text we search for to find the right table

        Returns
        -------
        Returns detailed information dataframe

        """

        results_row = self.browser.find_element(
            By.XPATH, "//td[text()='{}']/..".format(search_text)
        )

        results_row.click()

        address_table = WebDriverWait(
            self.browser, 10, ignored_exceptions=EC.StaleElementReferenceException
        ).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//td[contains(text(), '{}')]/ancestor::table[@class='table']".format(
                        search_by
                    ),
                )
            )
        )

        if address_table is None:
            return None

        table_df = pd.read_html(address_table.get_attribute("outerHTML"), index_col=0)[0].T

        if table_df.empty:
            return None

        try:
            address, status = (
                table_df["Company address:"].values.tolist(),
                table_df["Company status:"].values.tolist(),
            )

            return list(zip(address, status))

        except KeyError:
            return None

    def _search_data(
        self,
        search_text,
        search_by,
        loaded_text_css="[id$=gridCompanies][class='gridviewer_grid']",
        execute_css="[id$=btnSearchCompanies][class='button']",
        browser=None,
    ):
        """
        Searches the IMO website using the imo/company name given

        Parameters
        ----------
        search_text : name or imo of company
        search_by : CompanyName or ImoNumber
        loaded_text_css : css we look for to determine page is loaded
        execute_css : css we look for to click search
        browser : bvrowser object

        Returns
        -------
        HTML of found company information

        """

        if not browser:
            browser = self.browser

        browser.get(self.base)

        button_search = self._wait_for_object(item=execute_css, by=By.CSS_SELECTOR)

        if not button_search:
            return None

        COMPANY_SEARCH_CSS = "[id$=Company{}][type='text']".format(search_by)

        input_box = browser.find_element(By.CSS_SELECTOR, COMPANY_SEARCH_CSS)

        input_box.send_keys(search_text)

        search = WebDriverWait(browser, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, execute_css))
        )

        if not search:
            return None

        search.click()

        try:
            table = WebDriverWait(browser, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, loaded_text_css))
            )
        except TimeoutException:
            return None

        if table:
            return table.get_attribute("outerHTML")

        return None
