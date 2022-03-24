from models.models import *
import csv

import os
from os import listdir
from os.path import isfile, join
from datetime import datetime, timedelta

from base.db import session
from base.env import get_env
from sqlalchemy import desc
import requests


def initial_fill():
    """
    Fill PortCall table with manually downloaded data (from MarimeTraffic interface)
    Files are in assets/marinetraffic
    :return:
    """
    file_dir = "%s/assets/marinetraffic" % os.getcwd()
    ports = {}
    portcalls = {}
    departures = {}
    ships = {}

    for f in listdir(file_dir):
        if not isfile(join(file_dir, f)):
            continue

        with open(join(file_dir,f), newline='', encoding="utf-8-sig") as csvfile:
            rows  = csv.DictReader(csvfile, delimiter=',')
            count = 0
            for row in rows:
                if not ports.get(row['Port At Call Unlocode']):
                    ports[row['Port At Call Unlocode']]=_create_port_object(row)
                if not ships.get(row['Mmsi']):
                    ships[str(row['Mmsi'])] = _create_ship_object(row)

                key = "%s_%s"%(row['Mmsi'], row['Ata/atd'])
                if not portcalls.get(key):
                    portcalls[key] = _create_portcall_object(row)
                    
                key = "%s_%s_%s"%(row['Port At Call Unlocode'], row['Mmsi'], row['Ata/atd'])
                departures[key] = _create_departure_object(row)

    session.bulk_save_objects(ships.values())
    session.bulk_save_objects(ports.values())
    session.bulk_save_objects(portcalls.values())
    session.flush()
    for dep in departures.values():
        key = "%s_%s"%(dep.ship_mmsi, datetime.strftime(dep.date_utc, "%Y-%m-%d %H:%M:%S"))
        dep.portcall_id =portcalls[key].id

    session.bulk_save_objects(departures.values())

    session.commit()
            
    return {'ports':len(ports.keys()), 'departures':len(departures.keys()), 'ships':len(ships.keys()), 'portcalls':len(portcalls.keys())}

def _create_portcall_object(row):
    return PortCall(
        ship_mmsi = row['Mmsi'],
        move_type = str(int(row['Port Call Type']!='DEPARTURE')),
        type_name= row['Vessel Type - Generic'],
        port_unlocode = row['Port At Call Unlocode'],
        date_lt = datetime.strptime(row['Ata/atd'], "%Y-%m-%d %H:%M:%S"),
        date_utc = datetime.strptime(row['Ata/atd'], "%Y-%m-%d %H:%M:%S")
    )

def _create_port_object(row):
    return Port(
        unlocode=row['Port At Call Unlocode'],
        name=row['Port At Call'],
    )

def _create_ship_object(row):
    return Ship(
        mmsi = row['Mmsi'],
        name = row['Vessel Name'],
        imo = row['Imo'],
        type = row['Vessel Type - Detailed'],
        dwt = row['Capacity - Dwt'],
    )

def _create_terminal_object(row):
    pass

def _create_berth_object(row):
    pass

def _create_arrival_object(row):
    pass
    
def _create_departure_object(row):
    return Departure(
        port_id = row['Port At Call Unlocode'],
        ship_mmsi = row['Mmsi'],
        date_utc = datetime.strptime(row['Ata/atd'], "%Y-%m-%d %H:%M:%S")
    )




"""
Fill port calls for ports of interest, for dates not yet queried in database
:return:
"""
#TODO
# - query MarineTraffic for portcalls at ports of interest (assets/departure_port_of_interest.csv)
# - upsert in db
# Things to pay attention to:
# - we want to minimize credits use from MarineTraffic. So need to only query from last date available.
# - port call have foreignkeys towards Port and Ship tables. If Port or Ship are missing, we need to find
# information and insert them in their respective tables (probably using Datalastic)

def update():

    file_path = "%s/assets/departure_ports_of_interest.csv" % os.getcwd()
    ports = []
    with open(file_path, newline='', encoding="utf-8-sig") as csvfile:
        port_data  = csv.DictReader(csvfile, delimiter=',')
        for port in port_data:
            ports.append(port['unlocode'])

    latest_record = PortCall.query.order_by(desc(PortCall.date_utc)).limit(1).all()


    latest_date = datetime.now()-timedelta(days=1)
    if len(latest_record)>0:
        latest_date = latest_record[0].date_utc

    date_from_str = datetime.strftime(latest_date, "YYYY-MM-DD HH:MM")
    date_to_str = datetime.strftime(datetime.now(), "YYYY-MM-DD HH:MM")
    query_param = {}
    query_param['portid'] = ports
    query_param['movetype'] = 1
    query_param['fromdate'] = date_from_str
    query_param['todate'] = date_to_str
    query_param['protocol'] = 'json'
    query_param['v'] = 1



    url = "https://services.marinetraffic.com/api/portcalls/%s"%(get_env('MARINETRAFFIC_KEY'))
    print(url)
    #resp = requests.get(url, params=query_param)

    #if not resp.ok:
     #   raise Exception()
    
    #portcall_data = resp.json()
    portcall_data = [
        {
          "MMSI": "244770588",
          "SHIPNAME": "PIETERNELLA",
          "SHIP_ID": "3351323",
          "TIMESTAMP_LT": "2020-10-20T12:15:00.000Z",
          "TIMESTAMP_UTC": "2020-10-20T10:15:00.000Z",
          "MOVE_TYPE": "0",
          "TYPE_NAME": "Inland, Unknown",
          "PORT_ID": "1766",
          "PORT_NAME": "AMSTERDAM",
          "UNLOCODE": "NLAMS"
        },
        {
          "MMSI": "244690666",
          "SHIPNAME": "BRABANT",
          "SHIP_ID": "241767",
          "TIMESTAMP_LT": "2020-10-20T12:14:00.000Z",
          "TIMESTAMP_UTC": "2020-10-20T10:14:00.000Z",
          "MOVE_TYPE": "1",
          "TYPE_NAME": "Inland, Motor Freighter",
          "PORT_ID": "1766",
          "PORT_NAME": "AMSTERDAM",
          "UNLOCODE": "NLAMS"
        }
    ]

    ports = {}
    portcalls = {}
    departures = {}
    ships = {}


    #Checking for existing Port and Ships
    port_ids = {}
    ship_ids = {}

    for row in portcall_data:
        port_ids[str(row['UNLOCODE'])] = True
        ship_ids[str(row['MMSI'])] = True

    existing_ports = session.query(Port).filter(Port.unlocode.in_(list(port_ids.keys())))
    existing_ships = session.query(Ship).filter(Ship.mmsi.in_(list(ship_ids.keys())))

    for port in existing_ports:
        port_ids[str(port.unlocode)] = False

    for ship in existing_ships:
        ship_ids[str(ship.mmsi)] = False



    # Creating data to be inserted
    for row in portcall_data:
        key = row['UNLOCODE']

        if port_ids[key]:
            ports[key]=_create_port_object_from_portcall(row)
    
        key = row['MMSI']
        if ship_ids[key]:
            ships[str(key)] = _create_ship_object_from_portcall(row)

        obj = _create_portcall_object_from_portcall(row)
        key = "%s_%s"%(row['MMSI'], datetime.strftime(obj.date_utc, "%Y-%m-%d %H:%M:%S"))
        portcalls[key] = obj
            
        obj = _create_departure_object_from_portcall(row)
        key = "%s_%s_%s"%(row['UNLOCODE'], row['MMSI'], datetime.strftime(obj.date_utc, "%Y-%m-%d %H:%M:%S"))
        departures[key] = _create_departure_object_from_portcall(row)


    session.bulk_save_objects(ships.values())
    session.bulk_save_objects(ports.values())
    session.bulk_save_objects(portcalls.values())
    session.flush()

    for dep in departures.values():
        key = "%s_%s"%(dep.ship_mmsi, datetime.strftime(dep.date_utc, "%Y-%m-%d %H:%M:%S"))
        dep.portcall_id = portcalls[key].id

    session.bulk_save_objects(departures.values())

    session.commit()
            
    return {'ports':len(ports.keys()), 'departures':len(departures.keys()), 'ships':len(ships.keys()), 'portcalls':len(portcalls.keys())}


def _create_departure_object_from_portcall(row):
    return Departure(
        port_id = row['UNLOCODE'],
        ship_mmsi = row['MMSI'],
        date_utc = datetime.strptime(row['TIMESTAMP_UTC'], "%Y-%m-%dT%H:%M:%S.%fZ")
    )

def _create_ship_object_from_portcall(row):
    return Ship(
        mmsi = row['MMSI'],
        name = row['SHIPNAME'],
        imo = row['SHIP_ID'],
        type = row['TYPE_NAME'],
    )

def _create_port_object_from_portcall(row):
    return Port(
        unlocode=row['UNLOCODE'],
        name=row['PORT_NAME'],
    )

def _create_portcall_object_from_portcall(row):
    return PortCall(
        ship_mmsi = row['MMSI'],
        move_type = row['MOVE_TYPE'],
        type_name= row['TYPE_NAME'],
        port_unlocode = row['UNLOCODE'],
        date_lt = datetime.strptime(row['TIMESTAMP_LT'], "%Y-%m-%dT%H:%M:%S.%fZ"),
        date_utc = datetime.strptime(row['TIMESTAMP_UTC'], "%Y-%m-%dT%H:%M:%S.%fZ")
    )
    

def get(date_from=None):
    """

    :param date_from:
    :return: Pandas dataframe of portcalls
    """
    return
