from tqdm import tqdm
import pandas as pd
import datetime as dt
import sqlalchemy as sa

import base

from base.db_utils import execute_statement
from base.db import session
from base.env import get_env
from base.logger import logger
from base.models import (
    ShipInsurer,
    Company,
    Country,
)
from engines.company_scraper import CompanyImoScraper
from sqlalchemy.sql import update as update_sql


def clean_ship_details():
    fill_country()
    hide_invalid_insurance_entries()


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

    for company in tqdm(companies, unit="companies"):
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


def hide_invalid_insurance_entries():
    """
    Hide unknown insurance entries that overlap with known insurance entries.

    Returns
    -------

    """

    logger.info("Hiding unknown insurance entries that overlap with known entries.")

    unknown = (
        session.query(ShipInsurer)
        .filter(ShipInsurer.company_raw_name == "unknown", ShipInsurer.is_valid == True)
        .cte("unknown")
    )

    known = (
        session.query(ShipInsurer)
        .filter(ShipInsurer.company_raw_name != "unknown", ShipInsurer.is_valid == True)
        .cte("known")
    )

    problematic = (
        session.query(unknown.c.id)
        .outerjoin(known, unknown.c.ship_imo == known.c.ship_imo)
        .filter(
            sa.or_(
                known.c.date_from_equasis < unknown.c.date_from_equasis,
                known.c.date_from_equasis == None,
            ),
            known.c.updated_on > unknown.c.updated_on,
            unknown.c.updated_on - unknown.c.date_from_equasis < dt.timedelta(days=100),
        )
    ).cte("problematic")

    # Update ShipInsurers is_valid to false where in problematic
    statement = (
        update_sql(ShipInsurer).values({"is_valid": False}).where(ShipInsurer.id.in_(problematic))
    )

    execute_statement(statement)
