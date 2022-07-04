import pandas as pd
import geopandas as gpd
import shapely
from geoalchemy2 import func
import sqlalchemy as sa
import datetime as dt
from geoalchemy2 import Geometry
import sqlalchemy as sa
from sqlalchemy import func

import base
from base.db import session, engine
from base.logger import logger, logger_slack
from base.db_utils import upsert
from base.models import Ship, Departure, Shipment, AlertCriteria, ShipmentArrivalBerth, ShipmentDepartureBerth, Position, Arrival, Departure
from base.models import DB_TABLE_BERTH, DB_TABLE_SHIPMENTARRIVALBERTH, DB_TABLE_SHIPMENTDEPARTUREBERTH
from base.utils import to_list, to_datetime
from base.utils import update_geometry_from_wkb
from engine import port


def manual_alert(destination_iso2=None,
                 destination_name_pattern=None,
                 min_dwt=None,
                 commodity=None,
                 date_from=None):
    """
    A function to get what would be the resuts from an alert,
    without actually adding the alert_config and criteria in the db.
    Used to test alert on the frontend, for user to know roughly how many ships it would return.

    It should match the results of the build_alerts function below.

    :param destination_iso2s:
    :param delta_time:
    :return:
    """

    destination_iso2_field = func.unnest(Shipment.destination_iso2s).label('destination_iso2')
    destination_name_field = func.unnest(Shipment.destination_names).label('destination_name')
    destination_date_field = func.unnest(Shipment.destination_dates).label('destination_date')


    query = session.query(Shipment.id.label('shipment_id'),
                          Ship.imo,
                          Ship.dwt,
                          Ship.commodity,
                          destination_iso2_field,
                          destination_name_field,
                          destination_date_field) \
                .join(Departure, Departure.id == Shipment.departure_id) \
                .join(Ship, Ship.imo == Departure.ship_imo) \
                .subquery()

    prev_destination_iso2_field = func.lag(query.c.destination_iso2).over(
        partition_by=query.c.shipment_id,
        order_by=query.c.destination_date).label('previous_destination_iso2')

    prev_destination_name_field = func.lag(query.c.destination_name).over(
        partition_by=query.c.shipment_id,
        order_by=query.c.destination_date).label('previous_destination_name')

    query2 = session.query(query,
                           prev_destination_iso2_field,
                           prev_destination_name_field) \
             .subquery()

    query3 = session.query(query2) \
            .filter(sa.or_(query2.c.destination_iso2 != query2.c.previous_destination_iso2,
                           query2.c.destination_name != query2.c.previous_destination_name))

    if destination_iso2:
        query3 = query3.filter(
            sa.and_(query2.c.destination_iso2 != query2.c.previous_destination_iso2,
                    query2.c.destination_iso2.in_(to_list(destination_iso2)))
        )
    
    if destination_name_pattern:
        query3 = query3.filter(
            sa.and_(query2.c.destination_name != query2.c.previous_destination_name,
                    query2.c.destination_name.in_(to_list(destination_name_pattern)))  #TODO use pattern
        )

    if date_from:
        query3 = query3.filter(query2.c.destination_date >= to_datetime(date_from))

    if min_dwt:
        query3 = query3.filter(query2.c.dwt >= min_dwt)

    if commodity:
        query3 = query3.filter(query2.c.commodity.in_(to_list(commodity)))

    query3 = query3 \
                .order_by(query2.c.shipment_id, sa.desc(query2.c.destination_date)) \
                .distinct(query2.c.shipment_id)

    res = pd.read_sql(query3.statement, session.bind)
    return res


def build_alerts():

    query1 = session.query(Shipment.id,
                                       func.unnest(Shipment.destination_iso2s).label('destination_iso2'),
                                       func.unnest(Shipment.destination_names).label('destination_name'),
                                       func.unnest(Shipment.destination_dates).label('destination_date'),
                                       ).subquery()

    query2 = session.query(query1.c.id,
                           query1.c.destination_iso2,
                           func.lag(query1.c.destination_iso2).over(
                               partition_by=query1.c.id,
                               order_by=query1.c.destination_date)
                           .label('previous_destination_iso2'),
                           query1.c.destination_name,
                           query1.c.destination_date,
                           ).subquery()

    query3 = session.query(query2) \
                .filter(query2.c.destination_iso2 != query2.c.previous_destination_iso2) \
                .subquery()



    query4 = session.query(AlertCriteria.id,
                  func.unnest(AlertCriteria.new_destination_iso2).label('destination_iso2')) \
            .subquery()

    query5 = session.query(AlertCriteria.id, query3) \
                .join(query4, query4.c.id == AlertCriteria.id) \
                .join(query3, query3.c.destination_iso2 == query4.c.destination_iso2) \
                .filter(query3.c.destination_iso2 != sa.null())

    query6 = query5.distinct(AlertCriteria.id, query3.c.id.label('shipment_id'))

    a=pd.read_sql(query6.statement, session.bind)
    return
