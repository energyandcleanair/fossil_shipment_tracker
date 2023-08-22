import requests.exceptions
from tqdm import tqdm
import pandas as pd
import datetime as dt
from sqlalchemy import func
from sqlalchemy import nullslast
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from difflib import SequenceMatcher
import re
import base
import json

from base.db_utils import execute_statement
from base.encoder import JsonEncoder
from base.utils import to_list
from base.db import session
from base.env import get_env
from base.logger import logger, logger_slack
from base.models import (
    Commodity,
    Departure,
    ShipInsurer,
    ShipOwner,
    ShipManager,
    Company,
    Country,
    KplerProduct,
    KplerTrade,
    KplerZone,
    Ship,
    Port,
)
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


def update(imo=None, force_unknown=False):
    logger_slack.info("=== Company update ===")
    # For crude oil and oil products, force a daily refresh
    # given the importance for price caps and bans

    max_age = {
        base.CRUDE_OIL: 3,
        base.OIL_PRODUCTS: 3,
        base.OIL_OR_CHEMICAL: 3,
        base.LNG: 3,
        base.LPG: 3,
        base.COAL: 15,
        base.BULK: 15,
    }

    for commodity, max_age in max_age.items():
        logger.info("Updating %s" % commodity)
        update_info_from_equasis(
            imo=imo,
            commodities=to_list(commodity),
            last_updated=dt.datetime.now() - dt.timedelta(days=max_age),
            force_unknown=force_unknown,
        )

    fill_country()
    logger_slack.info("=== Company update done ===")
    return


def find_or_create_company_id(raw_name, imo=None, address=None):
    """
    The function checks whether we have a company which matches the name or exactly and has same imo, if not
    we attempt to create a record, and if there is imo conflict we double-check name similarity is close

    Parameters
    ----------
    raw_name : name of the company
    imo : optional imo of the company
    address : optional address of the company

    Returns
    -------

    """
    company_sq = session.query(
        Company.id, Company.imo, func.unnest(Company.names).label("name")
    ).subquery()
    existing_company = (
        session.query(company_sq)
        .filter(company_sq.c.name == raw_name, sa.or_(imo is None, company_sq.c.imo == imo))
        .first()
    )

    if existing_company:
        company_id = existing_company.id
    else:
        new_company = Company(
            imo=imo,
            name=raw_name,
            names=[raw_name],
            address=address,
            addresses=[address],
        )
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
                logger.warning(
                    "Inconsistency: %s != %s (IMO=%s)" % (existing_company.name, raw_name, imo)
                )
                company_id = None

    return company_id

def build_filter_query():

    departure_ships = (
        session.query(
            Departure.ship_imo.label("ship_imo"),
            Departure.date_utc.label("date_utc"),
            Departure.port_id.label("port_id"),
            Port.iso2.label("port_iso2"),
            Ship.commodity.label("commodity"),
            sa.sql.expression.literal("departure").label("source")
        )
        .outerjoin(
            Port, Departure.port_id == Port.id
        )
        .outerjoin(
            Ship, Ship.imo == Departure.ship_imo
        )
    )

    commodity_id_field = (
        "kpler_"
        + sa.func.replace(
            sa.func.replace(
                sa.func.lower(
                    func.coalesce(KplerProduct.commodity_name, KplerProduct.group_name)
                ),
                " ",
                "_",
            ),
            "/",
            "_",
        )
    ).label("commodity")

    kpler_ships = (
        session.query(
            func.unnest(KplerTrade.vessel_imos).label("ship_imo"),
            KplerTrade.departure_date_utc.label("date_utc"),
            KplerZone.port_id.label("port_id"),
            KplerZone.country_iso2.label("port_iso2"),
            Commodity.equivalent_id.label("commodity"),
            sa.sql.expression.literal("kpler").label("source")
        )
        .outerjoin(
            KplerZone, KplerTrade.departure_zone_id == KplerZone.id
        )
        .outerjoin(
            KplerProduct, KplerTrade.product_id == KplerProduct.id
        )
        .outerjoin(
            Commodity, commodity_id_field == Commodity.id
        )
    )

    filter_query = kpler_ships.union(departure_ships).subquery()

    return filter_query

def update_info_from_equasis(
    commodities=None,
    last_updated=dt.date.today() - dt.timedelta(days=base.REFRESH_COMPANY_DAYS),
    departure_date_from=None,
    imo=None,
    departure_port_id=None,
    departure_port_iso2=None,
    force_unknown=False,
):
    """
    Collect infos from equasis about shipments that either don't have infos,
    or for infos that are potentially outdated
    :return:
    """

    filter_query = build_filter_query()

    imo_query = (
        session.query(
            Ship.imo,
            ShipInsurer.company_raw_name,
            ShipInsurer.updated_on.label("last_updated")
        )
        .outerjoin(ShipInsurer, ShipInsurer.ship_imo == Ship.imo)
        .outerjoin(filter_query, filter_query.c.ship_imo == Ship.imo)
        .distinct(filter_query.c.ship_imo)
        .order_by(filter_query.c.ship_imo, nullslast(ShipInsurer.updated_on.desc()))
    )

    if commodities:
        imo_query = imo_query.filter(filter_query.c.commodity.in_(to_list(commodities)))

    if departure_date_from:
        imo_query = imo_query.filter(filter_query.c.date_utc >= departure_date_from)

    if imo:
        imo_query = imo_query.filter(filter_query.c.ship_imo.in_(to_list(imo)))

    if departure_port_id:
        imo_query = imo_query.filter(filter_query.c.port_id.in_(to_list(departure_port_id)))

    if departure_port_iso2:
        imo_query = imo_query.filter(filter_query.c.port_iso2.in_(to_list(departure_port_iso2)))

    imo_query = imo_query.subquery()

    imo_query = (
        session.query(
            imo_query
        )
        .filter(
            sa.or_(
                imo_query.c.last_updated <= last_updated,
                imo_query.c.last_updated == None,
                sa.and_(force_unknown, imo_query.c.company_raw_name == base.UNKNOWN_INSURER),
            )
        )
    )

    imos_results = imo_query.all()

    results = pd.DataFrame(imos_results)

    results = results[~results.imo.str.match("_v", case=False)]

    unique_imos = results.imo.unique()
    unique_imos_count = len(unique_imos)

    logger.info(f"{unique_imos_count} ship IMOs to update")

    imos = unique_imos
    ntries = 3

    if imos:
        equasis = Equasis()
    else:
        equasis = None

    for imo in tqdm(imos):
        itry = 0
        equasis_infos = None

        while equasis_infos is None and itry <= ntries:
            itry += 1
            try:
                imo_equasis = imo.replace("NOTFOUND_", "")
                equasis_infos = equasis.get_ship_infos(imo=imo_equasis)
            except requests.exceptions.HTTPError as e:
                logger.warning("Failed to get equasis ship info, trying again.")
            except requests.exceptions.ConnectionError as e:
                logger.warning("Connection failed, trying again.")

        if equasis_infos is not None:
            # Update ship record
            ship = session.query(Ship).filter(Ship.imo == imo).first()
            others = dict(ship.others) if ship.others else {}
            others.update({"equasis": equasis_infos})
            # To convert datetimes to str
            others = json.loads(json.dumps(others, cls=JsonEncoder))
            ship.others = others
            session.commit()

            # Insurer
            if equasis_infos.get("insurers"):
                equasis_insurers = equasis_infos.get("insurers")

                for equasis_insurer in equasis_insurers:
                    insurer_raw_name = equasis_insurer.get("name")
                    insurer_raw_date_from = equasis_insurer.get("date_from")
                    # See if exists
                    insurer = (
                        session.query(ShipInsurer)
                        .filter(
                            ShipInsurer.company_raw_name == insurer_raw_name,
                            ShipInsurer.ship_imo == imo,
                        )
                        .first()
                    )

                    if not insurer:
                        # If this is the first time we collect insurer for this ship,
                        # We assume it has always been this insurer
                        # This is important because we only start querying a ship insurer
                        # After we had a departure with it, and so the first insurer
                        # would always be after the first departure otherwise
                        has_insurer = (
                            session.query(ShipInsurer).filter(ShipInsurer.ship_imo == imo).count() > 0
                        )
                        date_from_ = insurer_raw_date_from or dt.datetime.now() if has_insurer else None
                        insurer = ShipInsurer(
                            company_raw_name=insurer_raw_name,
                            imo=None,
                            ship_imo=imo,
                            company_id=find_or_create_company_id(raw_name=insurer_raw_name),
                            date_from=date_from_,
                        )
                    insurer.updated_on = dt.datetime.now()
                    if insurer_raw_date_from and insurer.date_from is not None:
                        # HEURISTIC
                        # Very important assumption about equasis: we only update the date_from if it is not null
                        # It may happen indeed that it was the same insurer before the inception date
                        # and that the contract was only renewed
                        insurer.date_from = insurer_raw_date_from
                    session.add(insurer)
                    try:
                        session.commit()
                    except IntegrityError as e:
                        session.rollback()
                        logger.warning("Failed to add insurer %s for ship %s" % (insurer_raw_name, imo))

            # Manager
            manager_info = equasis_infos.get("manager")
            if manager_info:
                manager_raw_name = manager_info.get("name")
                manager_address = manager_info.get("address")
                manager_imo = manager_info.get("imo")
                manager_date_from = manager_info.get("date_from")

                # See if exists
                manager = (
                    session.query(ShipManager)
                    .filter(
                        ShipManager.company_raw_name == manager_raw_name,
                        ShipManager.imo == manager_imo,
                        ShipManager.ship_imo == imo,
                        ShipManager.date_from == manager_date_from,
                    )
                    .first()
                )
                if not manager:
                    manager = ShipManager(
                        company_raw_name=manager_raw_name,
                        ship_imo=imo,
                        imo=manager_imo,
                        date_from=manager_date_from,
                        company_id=find_or_create_company_id(
                            raw_name=manager_raw_name,
                            imo=manager_imo,
                            address=manager_address,
                        ),
                    )
                manager.updated_on = dt.datetime.now()
                session.add(manager)
                session.commit()

            # Owner
            owner_info = equasis_infos.get("owner")
            if owner_info:
                owner_raw_name = owner_info.get("name")
                owner_address = owner_info.get("address")
                owner_imo = owner_info.get("imo")
                owner_date_from = owner_info.get("date_from")

                # See if exists
                owner = (
                    session.query(ShipOwner)
                    .filter(
                        ShipOwner.company_raw_name == owner_raw_name,
                        ShipOwner.ship_imo == imo,
                        ShipOwner.date_from == owner_date_from,
                    )
                    .first()
                )
                if not owner:
                    owner = ShipOwner(
                        company_raw_name=owner_raw_name,
                        ship_imo=imo,
                        imo=owner_imo,
                        date_from=owner_date_from,
                        company_id=find_or_create_company_id(
                            raw_name=owner_raw_name,
                            imo=owner_imo,
                            address=owner_address,
                        ),
                    )
                owner.updated_on = dt.datetime.now()

                # Verify we DID find a matching company_id using find_or_create_company_id otherwise we will have an
                # integrity error
                if owner.company_id is not None:
                    session.add(owner)
                    session.commit()
                else:
                    logger.warning(
                        "Failed to find/create company_id for company {}, ship_imo {}.".format(
                            owner.company_raw_name, owner.ship_imo
                        )
                    )


def fill_country():
    """
    This function uses regex and company name/address to attempt to fill in country_iso2 and registration_country_iso2

    Returns
    -------

    """

    def fill_using_country_ending():
        """
        We check the ending of company address using regex to see if we can determine iso2

        Returns
        -------

        """
        country_regex = session.query(
            Country.iso2,
            ("[\.| |,|_|-|/]{1}" + Country.name + "[\.]?$").label("regexp"),
        ).subquery()
        update = (
            Company.__table__.update()
            .values(country_iso2=country_regex.c.iso2)
            .where(
                sa.and_(
                    Company.country_iso2 == sa.null(),
                    Company.address.op("~*")(country_regex.c.regexp),
                )
            )
        )
        execute_statement(update)

    def fill_using_address_regexps():
        """
        Using address to estimate country_iso2
        which might be different from registration_country_iso2

        Returns
        -------

        """
        address_regexps = {
            "US": ["USA[\.]?$"],
            "SG": ["Singapore [0-9]*$"],
            "TW": ["\(Taiwan\)[\.]?"],
            "PT": ["Madeira[\.]?$"],
            "HK": ["Hong Kong, China[\.]?[\w]*[0-9]*"],
            "IM": ["Isle of Man"],
            "JE": ["Jersey"],
        }

        for key, regexps in address_regexps.items():
            condition = sa.or_(*[Company.address.op("~")(regexp) for regexp in regexps])
            update = Company.__table__.update().values(country_iso2=key).where(condition)
            execute_statement(update)

    def fill_using_name_regexps():
        """
        This is for insurers. Assuming country == registration_country

        Returns
        -------

        """
        name_regexps = {
            "BM": ["\(Bermuda\)$"],
            "GB": [
                "Britannia Steamship insurance Association Ld",
                "North of England P&I Association",
                "UK P&I Club",
                "The London P&I Club",
                "The West of  England Shipowners",
                "Standard P&I Club per Charles Taylor & Co",
            ],
            "LU": ["The Ship owners' Mutual P&I Association \(Luxembourg\)"],
            "JP": ["Japan Ship Owners' P&I Association"],
            "NO": ["Norway$", "^Hydor AS$"],
            "SE": ["\(Swedish Club\)$"],
            "US": ["American Steamship Owner P&I association$"],
            "NL": ["Noord Nederlandsche P&I Club$"],
            "RU": ["VSK Insurance Company"],
        }

        for key, regexps in name_regexps.items():
            condition = sa.and_(
                Company.address == sa.null(),
                sa.or_(*[Company.name.op("~")(regexp) for regexp in regexps]),
            )
            update = (
                Company.__table__.update()
                .values(country_iso2=key, registration_country_iso2=key)
                .where(condition)
            )
            execute_statement(update)

    def remove_care_of():
        to_remove = ["^Care of"]
        condition = sa.and_(sa.or_(*[Company.address.op("~")(regexp) for regexp in to_remove]))
        update = Company.__table__.update().values(country_iso2=sa.null()).where(condition)
        execute_statement(update)

    def fill_using_file():
        """
        Manual listing of companies registriation countries

        Returns
        -------

        """
        companies_df = pd.read_csv("assets/companies.csv", dtype={"imo": str})
        companies_df = companies_df.dropna(subset=["imo", "registration_iso2"])
        imo_country = dict(zip(companies_df.imo, companies_df.registration_iso2))
        from sqlalchemy.sql import case

        session.query(Company).filter(Company.imo.in_(imo_country)).update(
            {
                Company.registration_country_iso2: case(
                    imo_country,
                    value=Company.imo,
                )
            },
            synchronize_session="fetch",
        )
        session.commit()

        # For those without imo
        companies_df = pd.read_csv("assets/companies.csv", dtype={"imo": str})
        companies_df = companies_df[pd.isna(companies_df.imo)]
        companies_df = companies_df.dropna(subset=["name", "registration_iso2"])
        name_country = dict(zip(companies_df.name, companies_df.registration_iso2))
        from sqlalchemy.sql import case

        session.query(Company).filter(
            Company.name.in_(name_country), Company.imo == sa.null()
        ).update(
            {
                Company.registration_country_iso2: case(
                    name_country,
                    value=Company.name,
                )
            },
            synchronize_session="fetch",
        )
        session.commit()

    fill_using_country_ending()
    fill_using_address_regexps()
    fill_using_name_regexps()
    # remove_care_of()
    fill_using_file()
    fill_using_imo_website()


def fill_using_imo_website():
    """
    Query companies with missing registration ISO2 and fill it in if found

    Returns
    -------

    """
    scraper = CompanyImoScraper(base_url=base.IMO_BASE_URL, service=None)

    scraper.initialise_browser(headless=True)

    if not scraper.perform_login(get_env("IMO_USER"), get_env("IMO_PASSWORD")):
        return False

    db_countries = dict(session.query(Country.name, Country.iso2).all())

    # some countries from IMO website are not the same as standard/official names in our db, so let's add them
    additional_countries = {
        "USA": "US",
        "United States of America": "US",
        "China, People's Republic of": "CN",
        "Korea, South": "KR",
        "Korea, North": "KP",
        "Virgin Islands, British": "VI",
        "Singapore": "SG",
        "Canary Islands": "ES",
        "Kyrgyzstan": "KG",
        "Taiwan": "TW",
        "Chinese Taipei": "TW",
        "Hong Kong, China": "HK",
        "Madeira": "PT",
        "St Kitts & Nevis": "KN",
        "Antigua & Barbuda": "AG",
        "Irish Republic": "IE",
        "St Vincent & The Grenadines": "VC",
    }

    country_dict = {**db_countries, **additional_countries}

    companies = (
        session.query(Company)
        .filter(sa.and_(Company.registration_country_iso2 == sa.null(), Company.imo != sa.null()))
        .all()
    )

    for company in tqdm(companies):
        # check imo website for company imo or name
        company_info = scraper.get_information(search_text=str(company.imo))

        if company_info is None or len(company_info) > 1:
            logger.warning(
                "Company not found, or more than one company with this search term ({}), skipping...".format(
                    company.imo
                )
            )
            continue

        company_info = company_info[0]
        # add reg iso2 to record and commit
        try:
            company.registration_country_iso2 = country_dict[company_info[0]]
            session.commit()
        except KeyError:
            logger.warning(
                "We did not find the ISO2 for imo {}, country {}. Considering adding manually.".format(
                    company.imo, company_info[0]
                )
            )
        except IndexError:
            logger.warning(
                "Failed to parse correct information from IMO website for {}.".format(company.imo)
            )


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
                self.service = Service(ChromeDriverManager().install())
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
