import pandas as pd
import datetime as dt
import sqlalchemy as sa
import numpy as np
from sqlalchemy import func
from sqlalchemy.orm import aliased
from sqlalchemy import case
from sqlalchemy.dialects.postgresql import array, ARRAY,BIGINT
from sqlalchemy import cast, Text

import base
from base.db import session, engine
from base.models import AlertInstance, Ship, Country, Shipment, AlertCriteria, Commodity, Port,\
        AlertConfig, AlertCriteriaAssociation, AlertRecipient, AlertRecipientAssociation, Departure, Arrival
from base.utils import to_list, to_datetime
from base.logger import logger_slack

from app import mail
from flask_mail import Message


def update():
    logger_slack.info("=== Alert update ===")
    alerts_df = get_new_alerts()
    emails_df = build_email_contents(alerts_df)
    send_emails(emails_df)
    save_alerts(alerts_df)


def manual_alert(destination_iso2=None,
                 destination_name_pattern=None,
                 min_dwt=None,
                 commodity=None,
                 date_from=None,
                 departure_port_id=None):
    """
    A function to get what would be the resuts from an alert,
    without actually adding the alert_config and criteria in the db.
    Used to test alert on the frontend, for user to know roughly how many ships it would return.

    It should match the results of the build_alerts function below.

    :param destination_iso2s:
    :param delta_time:
    :return:
    """

    DeparturePort = aliased(Port)
    ArrivalPort = aliased(Port)

    destination_iso2_field = func.unnest(Shipment.destination_iso2s).label('destination_iso2')
    destination_name_field = func.unnest(Shipment.destination_names).label('destination_name')
    destination_date_field = func.unnest(Shipment.destination_dates).label('destination_date')

    query = session.query(Shipment.id.label('shipment_id'),
                          Shipment.status,
                          Ship.imo,
                          Ship.name,
                          Ship.dwt,
                          Ship.commodity,
                          Departure.port_id.label('departure_port_id'),
                          DeparturePort.name.label('departure_port_name'),
                          destination_iso2_field,
                          destination_name_field,
                          destination_date_field,
                          ArrivalPort.iso2.label('arrival_iso2'),
                          Commodity.name.label('commodity_name')) \
        .join(Departure, Departure.id == Shipment.departure_id) \
        .join(DeparturePort, DeparturePort.id == Departure.port_id) \
        .outerjoin(Arrival, Arrival.id == Shipment.arrival_id) \
        .outerjoin(ArrivalPort, Arrival.port_id == ArrivalPort.id) \
        .join(Ship, Ship.imo == Departure.ship_imo) \
                .outerjoin(Commodity, Commodity.id == Ship.commodity) \
        .subquery()

    prev_destination_iso2_field = func.lag(query.c.destination_iso2).over(
        partition_by=query.c.shipment_id,
        order_by=query.c.destination_date).label('previous_destination_iso2')

    prev_destination_name_field = func.lag(query.c.destination_name).over(
        partition_by=query.c.shipment_id,
        order_by=query.c.destination_date).label('previous_destination_name')

    query2 = session.query(query,
                           prev_destination_iso2_field,
                           prev_destination_name_field,
                           Country.name.label('destination_country')
                           ) \
            .outerjoin(Country, Country.iso2 == query.c.destination_iso2) \
            .subquery()

    previous_country = aliased(Country)

    query3 = session.query(query2,
                           previous_country.name.label('previous_country')) \
            .outerjoin(previous_country, previous_country.iso2 == query2.c.previous_destination_iso2) \
            .filter(sa.or_(query2.c.destination_iso2 != query2.c.previous_destination_iso2,
                           query2.c.destination_name != query2.c.previous_destination_name))

    if destination_iso2:
        query3 = query3.filter(
            sa.or_(
                sa.and_(query2.c.destination_iso2 != query2.c.previous_destination_iso2,
                        query2.c.destination_iso2.in_(to_list(destination_iso2))),
                query2.c.arrival_iso2.in_(to_list(destination_iso2))
            )

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

    if departure_port_id:
        query3 = query3.filter(query2.c.departure_port_id.in_(to_list(departure_port_id)))

    query3 = query3 \
                .order_by(query2.c.shipment_id, sa.desc(query2.c.destination_date)) \
                .distinct(query2.c.shipment_id)

    res = pd.read_sql(query3.statement, session.bind)
    return res


def get_new_alerts():
    """
    This function is building the alerts by joining the criteria
    and shipment data (as opposed to manually building them like in manual_alert function)
    :return:
    """

    # We use a trick for unnest to keep values with null rows
    NULL_ARTIFACT = '__null__'
    NULL_ARTIFACT_BIGINT = -1

    DepartureCountry = aliased(Country)
    DeparturePort = aliased(Port)

    ArrivalCountry = aliased(Country)
    ArrivalPort = aliased(Port)

    min_date = dt.date.today() - dt.timedelta(days=7)

    query_shipment1 = session.query(Shipment.id.label('shipment_id'),
                                    Shipment.departure_id,
                                    Shipment.arrival_id,
                                    Shipment.status,
                                    func.unnest(Shipment.destination_iso2s).label('destination_iso2'),
                                    func.unnest(Shipment.destination_names).label('destination_name'),
                                    func.unnest(Shipment.destination_dates).label('destination_date'),
                                    ) \
        .filter(Shipment.status != base.UNDETECTED_ARRIVAL) \
        .subquery()

    query_shipment2 = session.query(
        query_shipment1.c.shipment_id,
        query_shipment1.c.destination_iso2,
        query_shipment1.c.status,
        func.lag(query_shipment1.c.destination_iso2).over(
                               partition_by=query_shipment1.c.shipment_id,
                               order_by=query_shipment1.c.destination_date)
            .label('previous_destination_iso2'),
        query_shipment1.c.destination_name,
        func.lag(query_shipment1.c.destination_name).over(
                                partition_by=query_shipment1.c.shipment_id,
                                order_by=query_shipment1.c.destination_date)
            .label('previous_destination_name'),
        query_shipment1.c.destination_date,
        Ship.commodity,
        Commodity.name.label('commodity_name'),
        Ship.dwt,
        Ship.imo,
        Ship.name[func.array_length(Ship.name, 1)].label('ship_name'),
        Departure.port_id.label('departure_port_id'),
        DeparturePort.name.label('departure_port_name'),
        DepartureCountry.name.label('departure_country'),
        Arrival.date_utc.label('arrival_date'),
        ArrivalPort.name.label('arrival_port_name'),
        ArrivalCountry.iso2.label('arrival_iso2'),
        ArrivalCountry.name.label('arrival_country')
       ) \
        .join(Departure, Departure.id == query_shipment1.c.departure_id) \
        .join(DeparturePort, Departure.port_id == DeparturePort.id) \
        .join(DepartureCountry, DepartureCountry.iso2 == DeparturePort.iso2) \
        .outerjoin(Arrival, Arrival.id == query_shipment1.c.arrival_id) \
        .outerjoin(ArrivalPort, Arrival.port_id == ArrivalPort.id) \
        .outerjoin(ArrivalCountry, ArrivalCountry.iso2 == ArrivalPort.iso2) \
        .join(Ship, Ship.imo == Departure.ship_imo) \
        .join(Commodity, Ship.commodity == Commodity.id) \
        .filter(query_shipment1.c.destination_date >= min_date) \
        .subquery()

    prev_country = aliased(Country)
    query_shipment3 = session.query(query_shipment2,
                                    Country.name.label('destination_country'),
                                    prev_country.name.label('previous_destination_country')) \
        .outerjoin(Country, Country.iso2 == query_shipment2.c.destination_iso2) \
        .outerjoin(prev_country, prev_country.iso2 == query_shipment2.c.previous_destination_iso2) \
        .filter(sa.or_(
            query_shipment2.c.destination_iso2 != query_shipment2.c.previous_destination_iso2,
            query_shipment2.c.destination_name != query_shipment2.c.previous_destination_name,
            query_shipment2.c.arrival_iso2 != sa.null()
        ))


    query_shipment = query_shipment3
    # shipment_df = pd.read_sql(query_shipment.statement, session.bind)
    query_shipment = query_shipment.subquery()

    # Unnesting and joining alert config, criteria, recipients
    query_alert1 = session.query(
            AlertConfig.id.label('config_id'),
            AlertConfig.name.label('config_name'),
            AlertCriteria.id.label('criteria_id'),
            AlertRecipient.id.label('recipient_id'),
            AlertRecipient.recipient.label('recipient_email'),
            AlertCriteria.commodity,
            AlertCriteria.min_dwt,
            AlertCriteria.new_destination_name_pattern,
            AlertCriteria.departure_port_ids,
            func.unnest(func.coalesce(AlertCriteria.new_destination_iso2, [NULL_ARTIFACT])).label('destination_iso2'),
        ) \
        .join(AlertCriteriaAssociation, AlertCriteriaAssociation.config_id == AlertConfig.id) \
        .join(AlertRecipientAssociation, AlertRecipientAssociation.config_id == AlertConfig.id) \
        .join(AlertCriteria, AlertCriteriaAssociation.criteria_id == AlertCriteria.id) \
        .join(AlertRecipient, AlertRecipientAssociation.recipient_id == AlertRecipient.id) \
        .filter(AlertRecipient.type == 'email') \
        .filter(AlertRecipient.recipient != sa.null()) \
        .filter(AlertRecipient.recipient != '')

    # alert1_df = pd.read_sql(query_alert1.statement, session.bind)
    query_alert1 = query_alert1.subquery()

    query_alert2 = session.query(
        query_alert1.c.config_id,
        query_alert1.c.config_name,
        query_alert1.c.criteria_id,
        query_alert1.c.recipient_id,
        query_alert1.c.recipient_email,
        query_alert1.c.destination_iso2,
        query_alert1.c.departure_port_ids,
        func.unnest(func.coalesce(
                    case([(query_alert1.c.new_destination_name_pattern == cast([], ARRAY(Text)), sa.null())], else_=query_alert1.c.new_destination_name_pattern),
                    [NULL_ARTIFACT])).label('destination_name_pattern'),
        query_alert1.c.commodity,
        query_alert1.c.min_dwt
        )

    # alert2_df = pd.read_sql(query_alert2.statement, session.bind)
    query_alert2 = query_alert2.subquery()

    query_alert3 = session.query(
        query_alert2.c.config_id,
        query_alert2.c.config_name,
        query_alert2.c.criteria_id,
        query_alert2.c.recipient_id,
        query_alert2.c.recipient_email,
        query_alert2.c.destination_iso2,
        query_alert2.c.departure_port_ids,
        query_alert2.c.destination_name_pattern,
        func.unnest(func.coalesce(
            case([(query_alert2.c.commodity == [], sa.null())], else_=query_alert2.c.commodity), [NULL_ARTIFACT])).label(
            'commodity'),
        query_alert2.c.min_dwt
    )

    # alert3_df = pd.read_sql(query_alert3.statement, session.bind)
    query_alert3 = query_alert3.subquery()

    query_alert4 = session.query(
        query_alert3.c.config_id,
        query_alert3.c.config_name,
        query_alert3.c.criteria_id,
        query_alert3.c.recipient_id,
        query_alert3.c.recipient_email,
        query_alert3.c.destination_iso2,
        query_alert3.c.destination_name_pattern,
        query_alert3.c.commodity,
        func.unnest(func.coalesce(
            case([(query_alert3.c.departure_port_ids == cast([], ARRAY(BIGINT)), sa.null())],
                 else_=query_alert3.c.departure_port_ids),
            [NULL_ARTIFACT_BIGINT])).label('departure_port_id'),
        query_alert3.c.min_dwt
    )

    # alert3_df = pd.read_sql(query_alert3.statement, session.bind)
    query_alert4 = query_alert4.subquery()

    query_alert = query_alert4

    # Join query alert and shipments
    query_alert_shipment = session.query(
        query_alert.c.config_id,
        query_alert.c.config_name,
        query_alert.c.criteria_id,
        query_alert.c.recipient_id,
        query_alert.c.recipient_email,
        query_shipment) \
        .join(query_shipment,
              sa.and_(
                  sa.or_(
                      sa.and_(query_alert.c.destination_iso2 == query_shipment.c.destination_iso2,
                              query_shipment.c.destination_iso2 != query_shipment.c.previous_destination_iso2),
                      query_alert.c.destination_iso2 == query_shipment.c.arrival_iso2,
                      query_alert.c.destination_iso2 == NULL_ARTIFACT),
                  sa.or_(
                      query_alert.c.departure_port_id == query_shipment.c.departure_port_id,
                      query_alert.c.departure_port_id == NULL_ARTIFACT_BIGINT),
                  sa.or_(query_alert.c.destination_name_pattern == query_shipment.c.destination_name,
                             query_alert.c.destination_name_pattern == NULL_ARTIFACT),
                  sa.or_(query_alert.c.commodity == query_shipment.c.commodity,
                         query_alert.c.commodity == NULL_ARTIFACT),
                  query_shipment.c.dwt >= query_alert.c.min_dwt
              )
        ).distinct(
            query_alert.c.config_id,
            query_alert.c.recipient_email,
            query_shipment.c.shipment_id
        )
    # alert_shipment_df = pd.read_sql(query_alert_shipment.statement, session.bind)
    query_alert_shipment = query_alert_shipment.subquery()

    # Query history alerts to only seek new shipments
    past_alerts = session.query(AlertInstance.config_id,
                                AlertInstance.recipient_id,
                                func.max(AlertInstance.date_utc).label('last_date_utc')) \
        .group_by(AlertInstance.config_id,
                  AlertInstance.recipient_id) \
        .subquery()

    query_alert_shipment_new = session.query(query_alert_shipment) \
        .outerjoin(past_alerts, sa.and_(past_alerts.c.config_id == query_alert_shipment.c.config_id,
                                   past_alerts.c.recipient_id == query_alert_shipment.c.recipient_id)) \
        .filter(sa.or_(past_alerts.c.last_date_utc == sa.null(),
                       query_alert_shipment.c.destination_date >= past_alerts.c.last_date_utc,
                       query_alert_shipment.c.arrival_date >= past_alerts.c.last_date_utc))

    alerts_df = pd.read_sql(query_alert_shipment_new.statement, session.bind)
    alerts_df.replace({NULL_ARTIFACT: None}, inplace=True)
    return alerts_df


def build_email_content(mail_alerts_df):

    table_html, table_txt = build_email_table(mail_alerts_df)
    config_id = mail_alerts_df.config_id.unique()[0]
    config_name = mail_alerts_df.config_name.unique()[0]
    recipient_email = mail_alerts_df.recipient_email.unique()[0]
    shipment_count = len(mail_alerts_df.shipment_id.unique())

    with open('assets/alert_email_template.html', 'r') as f:
        content_html = f.read()

    with open('assets/alert_email_template.txt', 'r') as f:
        content_txt = f.read()

    # Can't use .format because of {} in CSS
    content_html = content_html \
        .replace('{config_name}', config_name) \
        .replace('{config_id}', str(config_id)) \
        .replace('{alert_table}', table_html) \
        .replace('{recipient_email}', recipient_email) \
        .replace('{shipment_count}', str(shipment_count)) \
        .replace('{shipment_plural}', 's' if shipment_count > 1 else '')

    content_txt = content_txt \
        .replace('{config_name}', config_name) \
        .replace('{config_id}', str(config_id)) \
        .replace('{alert_table}', table_txt) \
        .replace('{recipient_email}', recipient_email) \
        .replace('{shipment_count}', str(shipment_count)) \
        .replace('{shipment_plural}', 's' if shipment_count > 1 else '')

    with open('email_%s_%s.html' % (recipient_email, config_name), 'w') as f:
        f.write(content_html)

    with open('email_%s_%s.txt' % (recipient_email, config_name), 'w') as f:
        f.write(content_txt)

    result = pd.DataFrame({
        'content_html': [content_html],
        'content_txt': [content_txt],
        'shipment_count': [shipment_count],
        'config_name': [config_name]})

    assert len(result) == 1
    return result


def build_email_table(alerts_df):
    # recipient_df = alert_shipment_df.loc[alert_shipment_df.recipient_email=='hubert@energyandcleanair.org']
    table_df = alerts_df \
            .sort_values(['destination_date'], ascending=False) \
            .drop_duplicates(['recipient_email', 'config_name', 'shipment_id'])
    table_df['dwt'] = table_df.dwt.apply(lambda x: "{:,.0f}".format(x))
    table_df['ship'] = table_df.ship_name + '<br><span class="commodity">' + table_df.commodity_name + '</span>' \
                       + '<br><span class="commodity">' + table_df.dwt + ' tonne</span>'
    table_df['status'] = table_df.status.str.title()
    table_df['departure'] = table_df.departure_port_name + '<br>' + table_df.departure_country
    table_df['arrival'] = table_df.arrival_port_name + '<br>' + table_df.arrival_country
    table_df['destination'] = table_df.destination_name + '<br>' + table_df.destination_country
    # If there's an arrival, use arrival infornation. If not, use destination
    table_df['destination'] = np.where(table_df.arrival_port_name.isnull(),
                                       table_df.destination,
                                       table_df.arrival)
    table_df['previous_destination'] = table_df.previous_destination_name + '<br>' + table_df.previous_destination_country

    def imo_to_links(imo):
        return """<a href='https://www.marinetraffic.com/en/ais/details/ships/{imo}' target='_blank'>Marine Traffic</a><br>
                <a href='https://www.vesseltracker.com/en/vessels.html?term={imo}' target='_blank'>Vessel Tracker</a>""" \
                .format(imo=imo).replace("\n","")

    table_df['links'] = table_df.imo.apply(imo_to_links)
    table_df['destination_date'] = table_df.destination_date.dt.strftime('%d %b %Y<br>%H:%M')

    table_columns = {
        'status': 'Status',
        'ship': 'Ship',
        'departure': 'Departure',
        'destination': 'Destination',
        'previous_destination': 'Previous destination',
        'destination_date': 'Date',
        'links': 'Links'
    }

    table_html = table_df[list(table_columns.keys())].rename(columns=table_columns) \
        .to_html(index=False, escape=False, classes='alert_table')

    table_txt = table_df[list(set(table_columns.keys()) - set(['links']))].rename(columns=table_columns) \
        .to_string(index=False)

    return table_html, table_txt


def build_email_contents(alerts_df):
    """
    Attach all email contents to the dataframe of alert_shipment
    :param alert_shipment_df:
    :return:
    """
    emails_df = alerts_df \
        .groupby(['recipient_email', 'config_id']) \
        .apply(build_email_content)

    return emails_df


def send_emails(emails_df):
    mails = emails_df.reset_index().to_dict(orient='records')
    logger_slack.info("Sending %d emails" % (len(mails),))
    for mail in mails:
        send_email(**mail)


def send_email(recipient_email, shipment_count, content_txt, config_name, content_html, **kwargs):
    from app import app
    with app.app_context():
        msg = Message('Russia Fossil Tracker: %d new shipment%s for your alert %s' % (shipment_count,
                                                                                 's' if shipment_count > 1 else '',
                                                                                 config_name),
                      recipients=[recipient_email])
        msg.body = content_txt
        msg.html = content_html
        mail.send(msg)


def save_alert(config_id, recipient_id):
    session.add(AlertInstance(config_id=config_id,
                              recipient_id=recipient_id))
    session.commit()
    return


def save_alerts(alerts_df):
    """
    Save the alerts that have been sent to avoid sending them again and again
    :param alerts_df:
    :return:
    """
    alerts_df[['config_id', 'recipient_id']] \
        .drop_duplicates() \
        .apply(lambda row: save_alert(row.config_id, row.recipient_id),
               axis=1)

    return