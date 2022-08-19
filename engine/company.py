from tqdm import tqdm
from sqlalchemy.exc import IntegrityError
import base
import datetime as dt
from sqlalchemy import func
import sqlalchemy as sa
from base.db_utils import execute_statement


from base.db import session
from base.logger import logger
from base.models import Ship, PortCall, Departure, Shipment, ShipInsurer, ShipOwner, ShipManager, Company, Country
from base.utils import to_datetime, to_list
from engine.datalastic import Datalastic
from engine.marinetraffic import Marinetraffic
from engine.equasis import Equasis



def update():
    update_info_from_equasis()
    fill_country()
    return

def find_or_create_company_id(raw_name, address=None):
    company_sq = session.query(Company.id,
                               func.unnest(Company.names).label('name')).subquery()
    existing_company = session.query(company_sq) \
        .filter(company_sq.c.name == raw_name) \
        .first()

    if existing_company:
        company_id = existing_company.id
    else:
        new_company = Company(name=raw_name,
                              names=[raw_name],
                              address=address,
                              addresses=[address])
        session.add(new_company)
        session.commit()
        company_id = new_company.id
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

    for imo in tqdm(imos):
        equasis_infos = equasis.get_ship_infos(imo=imo)
        if equasis_infos is not None:

            # Insurer
            if equasis_infos.get('insurer'):
                insurer_raw_name = equasis_infos.get('insurer').get('name')
                # See if exists
                insurer = session.query(ShipInsurer).filter(ShipInsurer.company_raw_name == insurer_raw_name,
                                                            ShipInsurer.ship_imo == imo).first()
                if not insurer:
                    insurer = ShipInsurer(company_raw_name=insurer_raw_name,
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
                manager_date_from = manager_info.get('date_from')

                # See if exists
                manager = session.query(ShipManager).filter(ShipManager.company_raw_name == manager_raw_name,
                                                        ShipManager.ship_imo == imo).first()
                if not manager:
                    manager = ShipManager(company_raw_name=manager_raw_name,
                                      ship_imo=imo,
                                      date_from=manager_date_from,
                                      company_id=find_or_create_company_id(raw_name=manager_raw_name,
                                                                           address=manager_address))
                manager.updated_on = dt.datetime.now()
                session.add(manager)
                session.commit()
                
            # Owner
            owner_info = equasis_infos.get('owner')
            if owner_info:
                owner_raw_name = owner_info.get('name')
                owner_address = owner_info.get('address')
                owner_date_from = owner_info.get('date_from')

                # See if exists
                owner = session.query(ShipOwner).filter(ShipOwner.company_raw_name == owner_raw_name,
                                                        ShipOwner.ship_imo == imo).first()
                if not owner:
                    owner = ShipOwner(company_raw_name=owner_raw_name,
                                      ship_imo=imo,
                                      date_from=owner_date_from,
                                      company_id=find_or_create_company_id(raw_name=owner_raw_name,
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
        address_regexps = {
            'US': ['USA[\.]?$'],
            'SG': ['Singapore [0-9]*$'],
            'TW': ['\(Taiwan\)[\.]?'],
            'HK': ['Hong Kong, China[\.]?[\w]*[0-9]*']
        }
        for key, regexps in address_regexps.items():
            condition = sa.or_(*[Company.address.op('~')(regexp) for regexp in regexps])
            update = Company.__table__.update().values(country_iso2=key) \
                .where(condition)
            execute_statement(update)

    def fill_using_name_regexps():
        # Only for those without address!
        name_regexps = {
            'BM': ['\(Bermuda\)$'],
            'NO': ['Norway$'],
            'GB': ['Britannia Steamship insurance Association Ld',
                   'North of England P&I Association',
                   'UK P&I Club',
                   'The London P&I Club',
                   'The West of  England Shipowners'],
            'LU': ['The Ship owners\' Mutual P&I Association \(Luxembourg\)'],
            'JP': ['Japan Ship Owners\' P&I Association'],
            'NO': ['^Hydor AS$'],
            'SE': ['\(Swedish Club\)$']
        }

        for key, regexps in name_regexps.items():
            condition = sa.and_(
                Company.address == sa.null(),
                sa.or_(*[Company.name.op('~')(regexp) for regexp in regexps]))
            update = Company.__table__.update().values(country_iso2=key) \
                .where(condition)
            execute_statement(update)

    fill_using_country_ending()
    fill_using_address_regexps()
    fill_using_name_regexps()





