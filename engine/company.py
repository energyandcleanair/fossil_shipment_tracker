import requests.exceptions
from tqdm import tqdm
import pandas as pd
import datetime as dt
from sqlalchemy import func
import sqlalchemy as sa
from base.db_utils import execute_statement
from difflib import SequenceMatcher


from base.db import session
from base.logger import logger
from base.models import Departure, ShipInsurer, ShipOwner, ShipManager, Company, Country
from engine.equasis import Equasis

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

import time
def update():
    update_info_from_equasis()
    fill_country()
    return


def find_or_create_company_id(raw_name, imo=None, address=None):

    company_sq = session.query(Company.id,
                               Company.imo,
                               func.unnest(Company.names).label('name')).subquery()
    existing_company = session.query(company_sq) \
        .filter(company_sq.c.name == raw_name,
                sa.or_(
                    imo is None,
                    company_sq.c.imo == imo)
                ) \
        .first()

    if existing_company:
        company_id = existing_company.id
    else:
        new_company = Company(imo=imo,
                              name=raw_name,
                              names=[raw_name],
                              address=address,
                              addresses=[address])
        session.add(new_company)
        try:
            session.commit()
            company_id = new_company.id
        except sa.exc.IntegrityError:
            session.rollback()
            existing_company = session.query(Company).filter(Company.imo == imo).first()
            ratio = SequenceMatcher(None, existing_company.name, raw_name).ratio()
            if ratio > 0.9:
                company_id = existing_company.id
            else:
                logger.warning('Inconsistency: %s != %s (IMO=%s)' % (existing_company.name, raw_name, imo))
                company_id = None

    return company_id


def update_info_from_equasis():
    """
    Collect infos from equasis about shipments that either don't have infos,
    or for infos that are potentially outdated
    :return:
    """
    max_age = dt.timedelta(days=31)
    equasis = Equasis()

    imos = session.query(Departure.ship_imo) \
        .outerjoin(ShipInsurer, ShipInsurer.ship_imo == Departure.ship_imo) \
        .outerjoin(ShipOwner, ShipOwner.ship_imo == Departure.ship_imo) \
        .outerjoin(ShipManager, ShipManager.ship_imo == Departure.ship_imo) \
        .filter(sa.or_(sa.and_(
                        ShipInsurer.id == sa.null(),
                        ShipOwner.id == sa.null(),
                        ShipManager.id == sa.null()),
                       ShipInsurer.updated_on <= dt.datetime.now() - max_age)) \
        .distinct() \
        .all()

    imos = [x[0] for x in imos]
    itry = 0
    ntries = 3
    equasis_infos = None

    for imo in tqdm(imos):

        while equasis_infos is None and itry <= ntries:
            itry += 1
            try:
                equasis_infos = equasis.get_ship_infos(imo=imo)
            except requests.exceptions.HTTPError as e:
                logger.warning("Failed to get equasis ship info, trying again.")

        if equasis_infos is not None:

            # Insurer
            if equasis_infos.get('insurer'):
                insurer_raw_name = equasis_infos.get('insurer').get('name')
                # See if exists
                insurer = session.query(ShipInsurer).filter(ShipInsurer.company_raw_name == insurer_raw_name,
                                                            ShipInsurer.ship_imo == imo).first()
                if not insurer:
                    insurer = ShipInsurer(company_raw_name=insurer_raw_name,
                                          imo=None,
                                          ship_imo=imo,
                                          company_id=find_or_create_company_id(raw_name=insurer_raw_name))
                insurer.updated_on = dt.datetime.now()
                session.add(insurer)
                session.commit()

            # Manager
            manager_info = equasis_infos.get('manager')
            if manager_info:
                manager_raw_name = manager_info.get('name')
                manager_address = manager_info.get('address')
                manager_imo = manager_info.get('imo')
                manager_date_from = manager_info.get('date_from')

                # See if exists
                manager = session.query(ShipManager).filter(ShipManager.company_raw_name == manager_raw_name,
                                                            ShipManager.imo == manager_imo,
                                                            ShipManager.ship_imo == imo).first()
                if not manager:
                    manager = ShipManager(company_raw_name=manager_raw_name,
                                          ship_imo=imo,
                                          imo=manager_imo,
                                          date_from=manager_date_from,
                                          company_id=find_or_create_company_id(raw_name=manager_raw_name,
                                                                               imo=manager_imo,
                                                                               address=manager_address))
                manager.updated_on = dt.datetime.now()
                session.add(manager)
                session.commit()
                
            # Owner
            owner_info = equasis_infos.get('owner')
            if owner_info:
                owner_raw_name = owner_info.get('name')
                owner_address = owner_info.get('address')
                owner_imo = owner_info.get('imo')
                owner_date_from = owner_info.get('date_from')

                # See if exists
                owner = session.query(ShipOwner).filter(ShipOwner.company_raw_name == owner_raw_name,
                                                        ShipOwner.ship_imo == imo).first()
                if not owner:
                    owner = ShipOwner(company_raw_name=owner_raw_name,
                                      ship_imo=imo,
                                      imo=owner_imo,
                                      date_from=owner_date_from,
                                      company_id=find_or_create_company_id(raw_name=owner_raw_name,
                                                                           imo=owner_imo,
                                                                           address=owner_address))
                owner.updated_on = dt.datetime.now()
                session.add(owner)
                session.commit()


def fill_country():

    def fill_using_country_ending():
        country_regex = session.query(Country.iso2,
                                      ('[\.| |,|_|-|/]{1}' + Country.name + '[\.]?$').label('regexp')).subquery()
        update = Company.__table__.update().values(country_iso2=country_regex.c.iso2) \
            .where(sa.and_(
                        Company.country_iso2 == sa.null(),
                        Company.address.op('~*')(country_regex.c.regexp)))
        execute_statement(update)

    def fill_using_address_regexps():
        """Using address to estimate country_iso2
        which might be different from registration_country_iso2
        """
        address_regexps = {
            'US': ['USA[\.]?$'],
            'SG': ['Singapore [0-9]*$'],
            'TW': ['\(Taiwan\)[\.]?'],
            'HK': ['Hong Kong, China[\.]?[\w]*[0-9]*'],
            'PT': ['Madeira[\.]?$']
        }

        for key, regexps in address_regexps.items():
            condition = sa.or_(*[Company.address.op('~')(regexp) for regexp in regexps])
            update = Company.__table__.update().values(country_iso2=key) \
                .where(condition)
            execute_statement(update)

    def fill_using_name_regexps():
        """
        This is for insurers. Assuming country == registration_country
        :return:
        """
        name_regexps = {
            'BM': ['\(Bermuda\)$'],
            'GB': ['Britannia Steamship insurance Association Ld',
                   'North of England P&I Association',
                   'UK P&I Club',
                   'The London P&I Club',
                   'The West of  England Shipowners',
                   'Standard P&I Club per Charles Taylor & Co'],
            'LU': ['The Ship owners\' Mutual P&I Association \(Luxembourg\)'],
            'JP': ['Japan Ship Owners\' P&I Association'],
            'NO': ['Norway$', '^Hydor AS$'],
            'SE': ['\(Swedish Club\)$'],
            'US': ['American Steamship Owner P&I association$'],
            'NL': ['Noord Nederlandsche P&I Club$'],
            'RU': ['VSK Insurance Company']
        }

        for key, regexps in name_regexps.items():
            condition = sa.and_(
                Company.address == sa.null(),
                sa.or_(*[Company.name.op('~')(regexp) for regexp in regexps]))
            update = Company.__table__.update().values(country_iso2=key,
                                                       registration_country_iso2=key) \
                .where(condition)
            execute_statement(update)


    def remove_care_of():
        to_remove = ['^Care of']
        condition = sa.and_(
            sa.or_(*[Company.address.op('~')(regexp) for regexp in to_remove]))
        update = Company.__table__.update().values(country_iso2=sa.null()) \
            .where(condition)
        execute_statement(update)


    def fill_using_file():
        """ Manual listing of companies registriation countries
        """
        companies_df = pd.read_csv("assets/companies.csv", dtype={'imo': str})
        companies_df = companies_df.dropna(subset=['imo', 'registration_iso2'])
        imo_country = dict(zip(companies_df.imo, companies_df.registration_iso2))
        from sqlalchemy.sql import case

        session.query(Company).filter(
            Company.imo.in_(imo_country)
        ).update({
            Company.registration_country_iso2: case(
                imo_country,
                value=Company.imo,
            )
        }, synchronize_session='fetch')
        session.commit()

        # For those without imo
        companies_df = pd.read_csv("assets/companies.csv", dtype={'imo': str})
        companies_df = companies_df[pd.isna(companies_df.imo)]
        companies_df = companies_df.dropna(subset=['name', 'registration_iso2'])
        name_country = dict(zip(companies_df.name, companies_df.registration_iso2))
        from sqlalchemy.sql import case

        session.query(Company).filter(
            Company.name.in_(name_country),
            Company.imo == sa.null()
        ).update({
            Company.registration_country_iso2: case(
                name_country,
                value=Company.name,
            )
        }, synchronize_session='fetch')
        session.commit()

    fill_using_country_ending()
    fill_using_address_regexps()
    fill_using_name_regexps()
    # remove_care_of()
    fill_using_file()


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
            element = WebDriverWait(browser, wait_time).until(EC.presence_of_element_located((by, item)))
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

        browser.get(self.base)

        AUTHOR_CSS = "[id$=AuthorityType][class='form-control']"
        LOGIN_FIELD_CSS = "[id$=txtUsername][class='form-control']"
        PWD_FIELD_CSS = "[id$=txtPassword][class='form-control']"
        LOGIN_BTN_CSS = "[id$=btnLogin][class='btn btn-default']"
        SRCH_BTN = "[id$=btnSearchCompanies][class='button']"

        button_search = self._wait_for_object(item=AUTHOR_CSS, by=By.CSS_SELECTOR)

        if not button_search:
            return False

        author_select = Select(browser.find_element(By.CSS_SELECTOR, AUTHOR_CSS))
        author_select.select_by_visible_text('Public Users')

        username_select = WebDriverWait(browser, 10, ignored_exceptions=EC.StaleElementReferenceException). \
            until(EC.element_to_be_clickable((By.CSS_SELECTOR, LOGIN_FIELD_CSS)))

        # selenium doesn't support webelement refresh, so we have to retry manually

        for i in range(0, 3):

            try:
                username_select = self.browser.find_element(By.CSS_SELECTOR, LOGIN_FIELD_CSS)

                ActionChains(browser) \
                    .click(username_select) \
                    .send_keys(username) \
                    .send_keys(Keys.ENTER) \
                    .perform()

                login_button = WebDriverWait(browser, 3, ignored_exceptions=EC.StaleElementReferenceException). \
                    until(EC.element_to_be_clickable((By.CSS_SELECTOR, LOGIN_BTN_CSS)))
            except TimeoutException:
                continue
            except EC.StaleElementReferenceException:
                continue

            break

        pwd_field = self.browser.find_element(By.CSS_SELECTOR, PWD_FIELD_CSS)

        if pwd_field is None:
            return False

        ActionChains(browser) \
            .click(pwd_field) \
            .send_keys(password) \
            .send_keys(Keys.ENTER) \
            .perform()

        # verify we logged in
        search_button = WebDriverWait(browser, 10, ignored_exceptions=EC.StaleElementReferenceException). \
            until(EC.element_to_be_clickable((By.CSS_SELECTOR, SRCH_BTN)))

        if not search_button:
            return False

        return True

    def initialise_browser(self, options=None, browser=None, headless=False):
        """
        Initialise web browser

        Parameters
        ----------
        options : webdriver options
        browser : webdriver options

        Returns
        -------

        """

        if not options:
            options = webdriver.ChromeOptions()
            options.add_argument('ignore-certificate-errors')
            if headless:
                options.add_argument("--headless")

        if not browser:
            if not self.service: self.service = Service(ChromeDriverManager().install())
            self.browser = webdriver.Chrome(service=self.service, options=options)
        else:
            self.browser = browser

    def get_information(self, search_text, search_by='ImoNumber', browser=None):
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

        table_html = self._search_data(search_text=search_text, search_by=search_by)

        if table_html:
            table_df = pd.read_html(table_html)[0]

            try:
                registration, name, imo = table_df['Registered in'].values[0], table_df['Name'].values[0], \
                                          table_df['IMO Company Number'].values[0]

                detailed_info = self.get_detailed_information(imo)
                address = detailed_info['Company address:'].values[0]

                return registration, address, name, imo

            except KeyError:
                return None

        return None

    def get_detailed_information(self, search_text):
        """
        Find the address of selected imo/name

        Parameters
        ----------
        search_text : base text we search for to find the right table

        Returns
        -------
        Returns detailed information dataframe

        """

        results_row = self.browser.find_element(By.XPATH, "//td[text()='{}']/..".format(search_text))

        results_row.click()

        address_table = WebDriverWait(self.browser, 10, ignored_exceptions=EC.StaleElementReferenceException). \
            until(
            EC.presence_of_element_located((By.XPATH, "//td[contains(text(), 'IMO')]/ancestor::table[@class='table']")))

        if address_table is None:
            return None

        return pd.read_html(address_table.get_attribute('outerHTML'), index_col=0)[0].T

    def _search_data(self,
                     search_text,
                     search_by,
                     loaded_text_css="[id$=gridCompanies][class='gridviewer_grid']",
                     execute_css="[id$=btnSearchCompanies][class='button']",
                     browser=None):
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

        search = WebDriverWait(browser, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, execute_css)))

        if not search:
            return None

        search.click()

        try:
            table = WebDriverWait(browser, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, loaded_text_css)))
        except TimeoutException:
            return None

        if table:
            return table.get_attribute('outerHTML')

        return None





