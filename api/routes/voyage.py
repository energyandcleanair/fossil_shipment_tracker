import datetime as dt
import pandas as pd
import geopandas as gpd
import re
import numpy as np
import sqlalchemy.sql.expression

from . import routes_api
from flask_restx import inputs


from base.models import Shipment, Ship, Arrival, Departure, Port, Berth,\
    ShipOwner, ShipInsurer, ShipManager, Company, \
    ShipmentDepartureBerth, ShipmentArrivalBerth, Commodity, Trajectory, \
    Destination, Price, Country, PortPrice, Currency, ShipmentWithSTS, Event
from base.db import session
from base.encoder import JsonEncoder
from base.utils import to_list, df_to_json, to_datetime
from base.logger import logger


from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import or_
from sqlalchemy import func
from base.utils import update_geometry_from_wkb
import country_converter as coco
import base
from engine.commodity import get_subquery as get_commodity_subquery


@routes_api.route('/v0/voyage', strict_slashes=False)
class VoyageResource(Resource):

    parser = reqparse.RequestParser()
    default_date_from = '2022-01-01'

    # Query content
    parser.add_argument('id', help='id(s) of voyage. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('commodity', help='commodity(ies) of interest. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('status', help='status of shipments. Could be any or several of completed, ongoing, undetected_arrival. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('date_from', help='start date for departure or arrival (format 2020-01-15)',
                        default=None, required=False)
    parser.add_argument('departure_date_from', help='start date for departure (format 2020-01-15)',
                        default=None, required=False)
    parser.add_argument('arrival_date_from', help='start date for arrival (format 2020-01-15)',
                        default=None, required=False)
    parser.add_argument('date_to', type=str, help='end date for departure or arrival arrival (format 2020-01-15 or -7 for seven days before today)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('ship_imo', action='split', help='IMO identifier(s) of the ship(s)',
                        required=False,
                        default=None)
    parser.add_argument('commodity_origin_iso2', action='split', help='iso2(s) of origin of commodity. Default to Russia only',
                        required=False,
                        default=None)
    parser.add_argument('departure_iso2', action='split', help='iso2(s) of departure (only RU should be available)',
                        required=False,
                        default=None)
    parser.add_argument('departure_port_id', action='split',
                        help='ids (CREA database id) of departure ports to consider',
                        required=False,
                        default=None)
    parser.add_argument('departure_berth_id', action='split',
                        help='ids (CREA database id) of departure berth to consider',
                        required=False,
                        default=None)
    parser.add_argument('departure_port_unlocode', action='split',
                        help='unlocode of departure ports to consider',
                        required=False,
                        default=None)
    parser.add_argument('destination_iso2', action='split', help='iso2(s) of destination',
                        required=False,
                        default=None)
    parser.add_argument('destination_region', action='split', help='region(s) of destination e.g. EU,Turkey',
                        required=False,
                        default=None)
    parser.add_argument('commodity_destination_iso2', action='split', help='ISO2(s) of commodity destination country',
                        required=False, default=None)
    parser.add_argument('commodity_destination_region', action='split',
                        help='region(s) of commodity destination e.g. EU28,Turkey',
                        required=False,
                        default=None)
    parser.add_argument('commodity_grouping', type=str,
                        help="Grouping used (e.g. coal,oil,gas ('default') vs coal,oil,lng,pipeline_gas ('split_gas')",
                        default='default')
    parser.add_argument('currency', action='split', help='currency(ies) of returned results e.g. EUR,USD,GBP',
                        required=False,
                        default=['EUR', 'USD'])
    parser.add_argument('routed_trajectory',
                        help='whether or not to use (re)routed trajectories for those that go over land (only applicable if format=geojson)',
                        required=False,
                        type=inputs.boolean, default=True)

    parser.add_argument('ship_owner_iso2', action='split', help='iso2(s) of ship owner',
                        required=False,
                        default=None)
    parser.add_argument('ship_owner_region', action='split', help='region(s) of ship owner e.g. EU,Turkey',
                        required=False,
                        default=None)

    parser.add_argument('ship_manager_iso2', action='split', help='iso2(s) of ship manager',
                        required=False,
                        default=None)
    parser.add_argument('ship_manager_region', action='split', help='region(s) of ship manager e.g. EU,Turkey',
                        required=False,
                        default=None)

    parser.add_argument('ship_insurer_iso2', action='split', help='iso2(s) of ship insurer',
                        required=False,
                        default=None)
    parser.add_argument('ship_insurer_region', action='split', help='region(s) of ship insurer e.g. EU,Turkey',
                        required=False,
                        default=None)


    # Query processing
    parser.add_argument('aggregate_by', type=str, action='split',
                        default=None,
                        help='which variables to aggregate by. Could be any of commodity, status, departure_date, arrival_date, departure_port, departure_country,'
                             'destination_port, destination_country, destination_region')
    parser.add_argument('rolling_days', type=int, help='rolling average window (in days). Default: no rolling averaging',
                        required=False, default=None)


    # Query format
    parser.add_argument('format', type=str, help='format of returned results (json, geojson or csv)',
                        required=False, default="json")
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('download', help='Whether to return results as a file or not.',
                        type=inputs.boolean, default=False)
    parser.add_argument('pivot_by', type=str,
                        help='pivoting value_eur (or any other specified by pivot_value) by e.g. commodity_group.',
                        required=False, action='split', default=None)
    parser.add_argument('pivot_value', type=str, help='pivoted value. Default: value_eur.',
                        required=False, default='value_eur')

    # Misc
    parser.add_argument('limit', type=int, help='how many result records do you want (default: keeps all)',
                        required=False, default=None)
    parser.add_argument('sort_by', type=str, help='sorting results e.g. asc(commodity),desc(value_eur)',
                        required=False, action='split', default=None)
    parser.add_argument('select', type=str,
                        help='selecting specific columns only, with the possibility to rename e.g. new_name1(var1),var2,var3',
                        required=False, action='split', default=None)

    @routes_api.expect(parser)
    def get(self):
        params = VoyageResource.parser.parse_args()
        return self.get_from_params(params)

    def get_from_params(self, params):
        id = params.get("id")
        commodity = params.get("commodity")
        status = params.get("status")
        date_from = params.get("date_from")
        departure_date_from = params.get("departure_date_from")
        arrival_date_from = params.get("arrival_date_from")
        commodity_origin_iso2 = params.get("commodity_origin_iso2")
        departure_iso2 = params.get("departure_iso2")
        departure_port_id = params.get("departure_port_id")
        departure_berth_id = params.get("departure_berth_id")
        departure_port_unlocode = params.get("departure_port_unlocode")
        destination_iso2 = params.get("destination_iso2")
        destination_region = params.get("destination_region")
        commodity_destination_iso2 = params.get("commodity_destination_iso2")
        commodity_destination_region = params.get("commodity_destination_region")
        commodity_grouping = params.get("commodity_grouping")

        ship_owner_iso2 = params.get("ship_owner_iso2")
        ship_owner_region = params.get("ship_owner_region")

        ship_manager_iso2 = params.get("ship_manager_iso2")
        ship_manager_region = params.get("ship_manager_region")

        ship_insurer_iso2 = params.get("ship_insurer_iso2")
        ship_insurer_region = params.get("ship_insurer_region")

        date_to = params.get("date_to")
        ship_imo = params.get("ship_imo")
        aggregate_by = params.get("aggregate_by")
        format = params.get("format", "json")
        nest_in_data = params.get("nest_in_data")
        download = params.get("download")
        rolling_days = params.get("rolling_days")
        routed_trajectory = params.get("routed_trajectory")
        currency = params.get("currency")
        sort_by = params.get("sort_by")
        limit = params.get("limit")
        pivot_by = params.get("pivot_by")
        pivot_value = params.get("pivot_value")
        select = params.get("select")

        DeparturePort = aliased(Port)
        ArrivalPort = aliased(Port)

        CommodityOriginCountry = aliased(Country)
        CommodityDestinationCountry = aliased(Country)

        ShipOwnerCompany = aliased(Company)
        ShipManagerCompany = aliased(Company)
        ShipInsurerCompany = aliased(Company)

        ShipOwnerCountry = aliased(Country)
        ShipManagerCountry = aliased(Country)
        ShipInsurerCountry = aliased(Country)

        DepartureCountry = aliased(Country)

        DepartureBerth= aliased(Berth)
        ArrivalBerth = aliased(Berth)

        DestinationPort = aliased(Port)
        DestinationCountry = aliased(Country)

        if aggregate_by and '' in aggregate_by:
            aggregate_by.remove('')

        # Commodity
        from sqlalchemy import case

        commodity_field = case(
            [
                (sa.and_(Ship.commodity.in_([base.BULK, base.GENERAL_CARGO]),
                         DepartureBerth.commodity.ilike('%coal%'),
                         # Lauri: For Taiwan, please exclude coal shipments without identified berth.
                         # I've done that for data I've provided to Taiwan because too much of the rest is iron ore, scrap etc
                         ArrivalPort.iso2 != 'TW'), 'coal'),

                (sa.and_(Ship.commodity.in_([base.BULK, base.GENERAL_CARGO]),
                         ArrivalPort.iso2 == 'TW',
                         DepartureBerth.commodity.ilike('%coal%'),
                         ArrivalBerth.id != sa.null()), 'coal'),

                (sa.and_(Ship.commodity.in_([base.BULK, base.GENERAL_CARGO]),
                         ArrivalBerth.commodity.ilike('%coal%')), 'coal'),

                (Ship.commodity.ilike('%bulk%'), 'bulk_not_coal')
            ],
            else_ = Ship.commodity
        ).label('commodity')

        # Price for all countries without country-specific price
        default_price = session.query(Price).filter(Price.country_iso2 == sa.null()).subquery()

        price_eur_per_tonne_field = (
            func.coalesce(PortPrice.eur_per_tonne, Price.eur_per_tonne, default_price.c.eur_per_tonne)
        ).label('price_eur_per_tonne')

        value_eur_field = (
            Ship.dwt * price_eur_per_tonne_field
        ).label('value_eur')

        # Technically, we could pivot long -> wide
        # but since we know there's a single ship per shipment
        # a rename will be faster
        value_tonne_field = case(
            [
                (Ship.unit == 'tonne', Ship.quantity),
            ],
            else_=Ship.dwt
        ).label('value_tonne')

        value_m3_field = case(
            [
                (Ship.unit == 'm3', Ship.quantity)
            ],
            else_=sa.null()
        ).label('value_m3')

        # Commodity origin and destination field
        commodity_origin_iso2_field = case(
            [(DepartureBerth.name == 'Novorossiysk CPC', 'KZ')],
            else_=DeparturePort.iso2
        ).label('commodity_origin_iso2')

        # To remove Kazak oil
        departure_iso2_field = case(
            [(DepartureBerth.name.ilike('Novorossiysk CPC%'), 'KZ')],
            else_=DeparturePort.iso2
        ).label('departure_iso2')

        destination_iso2_field = func.coalesce(ArrivalPort.iso2, Destination.iso2, DestinationPort.iso2) \
                                     .label('destination_iso2')

        value_currency_field = (value_eur_field * Currency.per_eur).label('value_currency')

        # combine sts shipment table with normal (non-sts) shipments

        DepartureShip = aliased(Ship)
        ArrivalShip = aliased(Ship)

        shipment_sts_departures = session.query(
                    ShipmentWithSTS,
                    Departure.event_id.label('departure_event_id')
                ) \
                .join(Departure, Departure.id == ShipmentWithSTS.departure_id) \
                .filter(Departure.event_id != sa.null()) \
                .subquery()

        shipment_sts_weights = session.query(
            ShipmentWithSTS.id,
                func.coalesce(ArrivalShip.dwt, func.avg(ArrivalShip.dwt).over(partition_by=ShipmentWithSTS.departure_id)).label('dwt_average'),
                func.sum(ArrivalShip.dwt).over(partition_by=ShipmentWithSTS.departure_id).label('dwt_total'),
                ArrivalShip.imo
        ) \
        .join(Departure, Departure.id == ShipmentWithSTS.departure_id) \
        .outerjoin(Arrival, Arrival.id == ShipmentWithSTS.arrival_id) \
        .join(DepartureShip, DepartureShip.imo == Departure.ship_imo) \
        .join(Event, Event.id == Arrival.event_id) \
        .outerjoin(ArrivalShip, ArrivalShip.imo == Event.interacting_ship_imo) \
        .filter(Arrival.event_id != sa.null()) \
        .subquery()

        shipments_sts_with_arrival = session.query(
            ShipmentWithSTS.id,
            ShipmentWithSTS.departure_id,
            shipment_sts_departures.c.arrival_id.label('arrival_id'),
            shipment_sts_departures.c.last_position_id,
            shipment_sts_departures.c.last_destination_name,
            shipment_sts_departures.c.status,
            shipment_sts_departures.c.destination_names,
            shipment_sts_departures.c.destination_dates,
            shipment_sts_departures.c.destination_iso2s,
            case(
                [(shipment_sts_weights.c.dwt_average != sa.null(), shipment_sts_weights.c.dwt_average / shipment_sts_weights.c.dwt_total)],
                else_=1.0
            ).label('weight'),
            sa.sql.expression.literal_column('True').label('is_sts'),
            ArrivalShip
        ) \
        .join(Departure, Departure.id == ShipmentWithSTS.departure_id) \
        .outerjoin(Arrival, Arrival.id == ShipmentWithSTS.arrival_id) \
        .outerjoin(shipment_sts_departures, shipment_sts_departures.c.departure_event_id == Arrival.event_id) \
        .outerjoin(shipment_sts_weights, shipment_sts_weights.c.id == ShipmentWithSTS.id) \
        .outerjoin(ArrivalShip, ArrivalShip.imo == shipment_sts_weights.c.imo) \
        .filter(Departure.event_id == sa.null())

        shipments_non_sts = session.query(
            Shipment,
            sa.sql.expression.literal_column('1.0').label('weight'),
            sa.sql.expression.literal_column('False').label('is_sts'),
            # Arrival ship is the same as departure ship for non sts shipments, so we just add this in so we can union
            Ship
        ) \
        .join(Departure, Departure.id == Shipment.departure_id) \
        .join(Ship, Ship.imo == Departure.ship_imo)

        shipments_combined = shipments_non_sts.union(shipments_sts_with_arrival).subquery()

        # generate commodity destination field now, after combining shipment tables

        commodity_destination_iso2_field = case(
            # Lauri: My heuristic is that all tankers that discharge cargo
            # in Yeosu but don't go to one of the identified berths are s2s
            [
                (sa.and_(
                    ArrivalPort.name.ilike('Yeosu%'),
                    commodity_field.in_([base.OIL_PRODUCTS, base.CRUDE_OIL, base.LNG,
                                         base.OIL_OR_CHEMICAL]),
                    ShipmentArrivalBerth.id == sa.null(),
                    ## Use below one once event_shipment has been fixed
                    # event_shipment_subquery.c.sts_shipment_id != sa.null()
                ), 'CN'),

                # Looks like StS only
                (ArrivalPort.name.ilike('Lakonikos Gulf%'), sa.null()),

                # For completed shipments, we don't use declared destination
                # but only actual one
                (shipments_combined.c.shipment_status == base.COMPLETED, ArrivalPort.iso2)
            ],
            else_=func.coalesce(ArrivalPort.iso2, Destination.iso2, DestinationPort.iso2)
        ).label('commodity_destination_iso2')

        commodity_subquery = get_commodity_subquery(session=session, grouping_name=commodity_grouping)

        # Query with joined information
        shipments_rich = (session.query(shipments_combined.c.shipment_id.distinct().label('id'), # Potential introducers of duplicates: ShipinOwner, Insurer etc.
                                        shipments_combined.c.shipment_status.label('status'),
                                        shipments_combined.c.is_sts,

                                    # Commodity origin and destination
                                    commodity_origin_iso2_field,
                                    CommodityOriginCountry.name.label('commodity_origin_country'),
                                    CommodityOriginCountry.region.label('commodity_origin_region'),
                                    commodity_destination_iso2_field,
                                    CommodityDestinationCountry.name.label('commodity_destination_country'),
                                    CommodityDestinationCountry.region.label('commodity_destination_region'),

                                    # Departure
                                    Departure.date_utc.label("departure_date_utc"),
                                    DeparturePort.unlocode.label("departure_unlocode"),
                                    departure_iso2_field,
                                    DepartureCountry.name.label("departure_country"),
                                    DepartureCountry.region.label("departure_region"),
                                    DeparturePort.name.label("departure_port_name"),
                                    DeparturePort.id.label("departure_port_id"),

                                    # Arrival
                                    Arrival.date_utc.label("arrival_date_utc"),
                                    ArrivalPort.unlocode.label("arrival_unlocode"),
                                    ArrivalPort.iso2.label("arrival_iso2"),
                                    ArrivalPort.name.label("arrival_port_name"),
                                    ArrivalPort.id.label("arrival_port_id"),

                                    # Intermediary destinations
                                    Destination.name.label("destination_name"),
                                    destination_iso2_field,
                                    DestinationCountry.name.label("destination_country"),
                                    DestinationCountry.region.label("destination_region"),

                                    shipments_combined.c.shipment_destination_names.label("destination_names"),
                                    shipments_combined.c.shipment_destination_dates.label("destination_dates"),
                                    shipments_combined.c.shipment_destination_iso2s.label("destination_iso2s"),

                                    Ship.name.label("ship_name"),
                                    Ship.imo.label("ship_imo"),
                                    Ship.mmsi.label("ship_mmsi"),
                                    Ship.type.label("ship_type"),
                                    Ship.subtype.label("ship_subtype"),
                                    Ship.dwt.label("ship_dwt"),

                                    shipments_combined.c.ship_name.label("arrival_ship_name"),
                                    shipments_combined.c.ship_imo.label("arrival_ship_imo"),
                                    shipments_combined.c.ship_mmsi.label("arrival_ship_mmsi"),
                                    shipments_combined.c.ship_type.label("arrival_ship_type"),
                                    shipments_combined.c.ship_subtype.label("arrival_ship_subtype"),
                                    shipments_combined.c.ship_dwt.label("arrival_ship_dwt"),

                                    commodity_field,
                                    commodity_subquery.c.group.label("commodity_group"),


                                    # Companies
                                    ShipManagerCompany.name.label("ship_manager"),
                                    ShipManagerCompany.imo.label("ship_manager_imo"),
                                    ShipManagerCountry.iso2.label("ship_manager_iso2"),
                                    ShipManagerCountry.name.label("ship_manager_country"),
                                    ShipManagerCountry.region.label("ship_manager_region"),

                                    ShipOwnerCompany.name.label("ship_owner"),
                                    ShipOwnerCompany.imo.label("ship_owner_imo"),
                                    ShipOwnerCountry.iso2.label("ship_owner_iso2"),
                                    ShipOwnerCountry.name.label("ship_owner_country"),
                                    ShipOwnerCountry.region.label("ship_owner_region"),

                                    ShipInsurerCompany.name.label("ship_insurer"),
                                    ShipInsurerCompany.imo.label("ship_insurer_imo"),
                                    ShipInsurerCountry.iso2.label("ship_insurer_iso2"),
                                    ShipInsurerCountry.name.label("ship_insurer_country"),
                                    ShipInsurerCountry.region.label("ship_insurer_region"),

                                    value_tonne_field.label('value_tonne_unweighted'),
                                    value_m3_field.label('value_m3_unweighted'),
                                    value_eur_field.label('value_eur_unweighted'),

                                    shipments_combined.c.weight.label('weight'),

                                    sa.sql.label('value_tonne', value_tonne_field*shipments_combined.c.weight),
                                    sa.sql.label('value_m3', value_m3_field*shipments_combined.c.weight),
                                    sa.sql.label('value_eur', value_eur_field*shipments_combined.c.weight),
                                    Currency.currency,
                                    value_currency_field,

                                    DepartureBerth.id.label("departure_berth_id"),
                                    DepartureBerth.name.label("departure_berth_name"),
                                    DepartureBerth.commodity.label("departure_berth_commodity"),
                                    DepartureBerth.port_unlocode.label("departure_berth_unlocode"),
                                    ArrivalBerth.id.label("arrival_berth_id"),
                                    ArrivalBerth.name.label("arrival_berth_name"),
                                    ArrivalBerth.owner.label("arrival_berth_owner"),
                                    ArrivalBerth.commodity.label("arrival_berth_commodity"),
                                    ArrivalBerth.port_unlocode.label("arrival_berth_unlocode"))

             .join(Departure, shipments_combined.c.shipment_departure_id == Departure.id)
             .join(DeparturePort, Departure.port_id == DeparturePort.id)
             .outerjoin(Arrival, shipments_combined.c.shipment_arrival_id == Arrival.id)
             .outerjoin(ArrivalPort, Arrival.port_id == ArrivalPort.id)
             .join(Ship, Departure.ship_imo == Ship.imo)
             .outerjoin(ShipmentDepartureBerth, shipments_combined.c.shipment_id == ShipmentDepartureBerth.shipment_id)
             .outerjoin(ShipmentArrivalBerth, shipments_combined.c.shipment_id == ShipmentArrivalBerth.shipment_id)
             .outerjoin(DepartureBerth, DepartureBerth.id == ShipmentDepartureBerth.berth_id)
             .outerjoin(ArrivalBerth, ArrivalBerth.id == ShipmentArrivalBerth.berth_id)
             .outerjoin(Destination, shipments_combined.c.shipment_last_destination_name == Destination.name)
             .outerjoin(DestinationPort, Destination.port_id == DestinationPort.id)
             .outerjoin(commodity_subquery, commodity_subquery.c.id == commodity_field)
             .outerjoin(Price,
                        sa.and_(Price.date == func.date_trunc('day', Departure.date_utc),
                                Price.commodity == commodity_subquery.c.pricing_commodity,
                                sa.or_(
                                    sa.and_(Price.country_iso2 == sa.null(), destination_iso2_field == sa.null()),
                                    Price.country_iso2 == destination_iso2_field)
                                )
                        )
             .outerjoin(default_price,
                         sa.and_(default_price.c.date == func.date_trunc('day', Departure.date_utc),
                                 default_price.c.commodity == commodity_subquery.c.pricing_commodity
                                 )
                        )
             .outerjoin(PortPrice,
                        sa.and_(
                            PortPrice.port_id == DeparturePort.id,
                            PortPrice.commodity == commodity_subquery.c.pricing_commodity,
                            PortPrice.date == func.date_trunc('day', Departure.date_utc)
                        ))
             .outerjoin(Currency, Currency.date == func.date_trunc('day', Departure.date_utc))
             .outerjoin(CommodityOriginCountry, CommodityOriginCountry.iso2 == commodity_origin_iso2_field)
             .outerjoin(CommodityDestinationCountry, CommodityDestinationCountry.iso2 == commodity_destination_iso2_field)
             .outerjoin(DestinationCountry, DestinationCountry.iso2 == destination_iso2_field)

             .outerjoin(ShipOwner, sa.and_(
                            ShipOwner.ship_imo == Departure.ship_imo,
                            sa.or_(
                                    Departure.date_utc >= ShipOwner.date_from,
                                    ShipOwner.date_from == sa.null())))
             .outerjoin(ShipOwnerCompany, ShipOwner.company_id == ShipOwnerCompany.id)
             .outerjoin(ShipOwnerCountry, ShipOwnerCompany.country_iso2 == ShipOwnerCountry.iso2)

             .outerjoin(ShipManager, sa.and_(
                    ShipManager.ship_imo == Departure.ship_imo,
                    sa.or_(
                        Departure.date_utc >= ShipManager.date_from,
                        ShipManager.date_from == sa.null())))
             .outerjoin(ShipManagerCompany, ShipManager.company_id == ShipManagerCompany.id)
             .outerjoin(ShipManagerCountry, ShipManagerCompany.country_iso2 == ShipManagerCountry.iso2)

             .outerjoin(ShipInsurer, sa.and_(
                    ShipInsurer.ship_imo == Departure.ship_imo,
                    sa.or_(
                        Departure.date_utc >= ShipInsurer.date_from,
                        ShipInsurer.date_from == sa.null())))
             .outerjoin(ShipInsurerCompany, ShipInsurer.company_id == ShipInsurerCompany.id)
             .outerjoin(ShipInsurerCountry, ShipInsurerCompany.country_iso2 == ShipInsurerCountry.iso2)

             .join(DepartureCountry, departure_iso2_field == DepartureCountry.iso2)
             .filter(destination_iso2_field != "RU"))

        if id is not None:
            shipments_rich = shipments_rich.filter(shipments_combined.c.shipment_id.in_(id))

        if ship_imo is not None:
            shipments_rich = shipments_rich.filter(Ship.imo.in_(to_list(ship_imo)))

        if commodity is not None:
            shipments_rich = shipments_rich.filter(commodity_field.in_(to_list(commodity)))

        if status is not None:
            shipments_rich = shipments_rich.filter(shipments_combined.c.shipment_status.in_(status))

        if date_from is not None:
            shipments_rich = shipments_rich.filter(
                sa.or_(
                    Departure.date_utc >= to_datetime(date_from)
                ))

        if departure_date_from is not None:
            shipments_rich = shipments_rich.filter(Departure.date_utc >= to_datetime(departure_date_from))

        if arrival_date_from is not None:
            shipments_rich = shipments_rich.filter(Arrival.date_utc >= to_datetime(arrival_date_from))

        # Add the default date_from if none has been specified
        if not date_from and not departure_date_from and not arrival_date_from:
            shipments_rich = shipments_rich.filter(Departure.date_utc >= to_datetime(VoyageResource.default_date_from))

        if date_to is not None:
            shipments_rich = shipments_rich.filter(
                sa.or_(
                    # Arrival.date_utc <= dt.datetime.strptime(date_to, "%Y-%m-%d"),
                    Departure.date_utc <= to_datetime(date_to)
                ))

        if departure_iso2 is not None:
            shipments_rich = shipments_rich.filter(DeparturePort.iso2.in_(to_list(departure_iso2)))

        if departure_port_id is not None:
            shipments_rich = shipments_rich.filter(DeparturePort.id.in_(to_list(departure_port_id)))

        if departure_berth_id is not None:
            shipments_rich = shipments_rich.filter(DepartureBerth.id.in_(to_list(departure_berth_id)))

        if departure_port_unlocode is not None:
            shipments_rich = shipments_rich.filter(DeparturePort.unlocode.in_(to_list(departure_port_unlocode)))

        if destination_iso2 is not None:
            shipments_rich = shipments_rich.filter(destination_iso2_field.in_(to_list(destination_iso2)))

        if destination_region is not None:
            shipments_rich = shipments_rich.filter(DestinationCountry.region.in_(to_list(destination_region)))

        if commodity_origin_iso2 is not None:
            shipments_rich = shipments_rich.filter(CommodityOriginCountry.iso2.in_(to_list(commodity_origin_iso2)))

        if commodity_destination_iso2 is not None:
            shipments_rich = shipments_rich.filter(CommodityDestinationCountry.iso2.in_(to_list(commodity_destination_iso2)))

        if commodity_destination_region is not None:
            shipments_rich = shipments_rich.filter(CommodityDestinationCountry.region.in_(to_list(commodity_destination_region)))

        if ship_owner_iso2 is not None:
            shipments_rich = shipments_rich.filter(
                ShipOwnerCountry.iso2.in_(to_list(ship_owner_iso2)))

        if ship_owner_region is not None:
            shipments_rich = shipments_rich.filter(
                ShipOwnerCountry.region.in_(to_list(ship_owner_region)))

        if ship_manager_iso2 is not None:
            shipments_rich = shipments_rich.filter(
                ShipManagerCountry.iso2.in_(to_list(ship_manager_iso2)))

        if ship_manager_region is not None:
            shipments_rich = shipments_rich.filter(
                ShipManagerCountry.region.in_(to_list(ship_manager_region)))

        if ship_insurer_iso2 is not None:
            shipments_rich = shipments_rich.filter(
                ShipInsurerCountry.iso2.in_(to_list(ship_insurer_iso2)))

        if ship_insurer_region is not None:
            shipments_rich = shipments_rich.filter(
                ShipInsurerCountry.region.in_(to_list(ship_insurer_region)))

        if currency is not None:
            shipments_rich = shipments_rich.filter(Currency.currency.in_(to_list(currency)))

        # Aggregate
        query = self.aggregate(query=shipments_rich, aggregate_by=aggregate_by)

        # Query
        result = pd.read_sql(query.statement, session.bind)

        if len(result) == 0:
            return Response(
                status=HTTPStatus.NO_CONTENT,
                response="empty",
                mimetype='application/json')

        # Rolling average
        result = self.roll_average(result=result, aggregate_by=aggregate_by, rolling_days=rolling_days)

        # Spread currencies
        result = self.spread_currencies(result=result)

        # Sort results
        result = self.sort_result(result=result, sort_by=sort_by)

        # Keep only n records
        if limit:
            result = result[:limit]

        # Pivot
        result = self.pivot_result(result=result, pivot_by=pivot_by, pivot_value=pivot_value)

        # Select, rename
        result = self.select(result, select=select)

        response = self.build_response(result=result, format=format, nest_in_data=nest_in_data,
                                       aggregate_by=aggregate_by, download=download, routed_trajectory=routed_trajectory)
        return response


    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user parameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Aggregate
        value_cols = [
            func.sum(subquery.c.ship_dwt).label("ship_dwt"),
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_m3).label("value_m3"),
            func.sum(subquery.c.value_eur).label("value_eur"),
            func.sum(subquery.c.value_currency).label("value_currency"),
            func.count(subquery.c.id).label("count"),
        ]

        # Adding must have grouping columns
        must_group_by = ['currency']
        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if '' in aggregate_by:
            aggregate_by.remove('')
        # Aggregating
        aggregateby_cols_dict = {
            'currency': [subquery.c.currency],
            'commodity': [subquery.c.commodity, subquery.c.commodity_group],
            'commodity_group': [subquery.c.commodity_group],

            'commodity_origin_iso2': [subquery.c.commodity_origin_iso2, subquery.c.commodity_origin_country, subquery.c.commodity_origin_region],
            'commodity_origin_country': [subquery.c.commodity_origin_iso2, subquery.c.commodity_origin_country,
                                      subquery.c.commodity_origin_region],
            'commodity_destination_iso2': [subquery.c.commodity_destination_iso2, subquery.c.commodity_destination_country, subquery.c.commodity_destination_region],
            'commodity_destination_country': [subquery.c.commodity_destination_iso2,
                                           subquery.c.commodity_destination_country,
                                           subquery.c.commodity_destination_region],
            'commodity_destination_region': [subquery.c.commodity_destination_region],

            'status': [subquery.c.status],

            'date': [func.date_trunc('day', subquery.c.departure_date_utc).label("departure_date")],
            'month': [func.date_trunc('month', subquery.c.departure_date_utc).label("departure_month")],
            'year': [func.date_trunc('year', subquery.c.departure_date_utc).label("departure_year")],

            'departure_date': [func.date_trunc('day', subquery.c.departure_date_utc).label("departure_date")],
            'departure_month': [func.date_trunc('month', subquery.c.departure_date_utc).label("departure_month")],
            'departure_year': [func.date_trunc('year', subquery.c.departure_date_utc).label("departure_year")],

            'arrival_date': [func.date_trunc('day', subquery.c.arrival_date_utc).label('arrival_date')],
            'arrival_month': [func.date_trunc('month', subquery.c.arrival_date_utc).label('arrival_month')],
            'arrival_year': [func.date_trunc('year', subquery.c.arrival_date_utc).label('arrival_year')],

            'departure_port': [subquery.c.departure_port_name, subquery.c.departure_unlocode,
                               subquery.c.departure_iso2, subquery.c.departure_country, subquery.c.departure_region],
            'departure_country': [subquery.c.departure_iso2, subquery.c.departure_country, subquery.c.departure_region],
            'departure_iso2': [subquery.c.departure_iso2, subquery.c.departure_country, subquery.c.departure_region],

            'destination_port': [subquery.c.arrival_port_name, subquery.c.arrival_unlocode,
                                 subquery.c.destination_iso2, subquery.c.destination_country, subquery.c.destination_region],
            'destination_country': [subquery.c.destination_iso2, subquery.c.destination_country, subquery.c.destination_region],
            'destination_iso2': [subquery.c.destination_iso2, subquery.c.destination_country, subquery.c.destination_region],
            'destination_region': [subquery.c.destination_region],

            'arrival_berth_owner': [subquery.c.arrival_berth_owner],

            'arrival_port': [subquery.c.arrival_port_id, subquery.c.arrival_port_name],
            'departure_port': [subquery.c.departure_port_id, subquery.c.departure_port_name],

            'ship_insurer': [subquery.c.ship_insurer, subquery.c.ship_insurer_imo, subquery.c.ship_insurer_country, subquery.c.ship_insurer_iso2, subquery.c.ship_insurer_region],
            'ship_manager': [subquery.c.ship_manager, subquery.c.ship_manager_imo, subquery.c.ship_manager_country, subquery.c.ship_manager_iso2, subquery.c.ship_manager_region],
            'ship_owner': [subquery.c.ship_owner, subquery.c.ship_owner_imo, subquery.c.ship_owner_country, subquery.c.ship_owner_iso2, subquery.c.ship_owner_region],

            'ship_insurer_country': [subquery.c.ship_insurer_country, subquery.c.ship_insurer_iso2,
                             subquery.c.ship_insurer_region],
            'ship_manager_country': [subquery.c.ship_manager_country, subquery.c.ship_manager_iso2,
                             subquery.c.ship_manager_region],
            'ship_owner_country': [subquery.c.ship_owner_country, subquery.c.ship_owner_iso2,
                           subquery.c.ship_owner_region],

            'ship_insurer_region': [subquery.c.ship_insurer_region],
            'ship_manager_region': [subquery.c.ship_manager_region],
            'ship_owner_region': [subquery.c.ship_owner_region]
        }

        if any([x not in aggregateby_cols_dict for x in aggregate_by]):
            logger.warning("aggregate_by can only be a selection of %s" % (",".join(aggregateby_cols_dict.keys())))
            aggregate_by = [x for x in aggregate_by if x in aggregateby_cols_dict]

        groupby_cols = []
        for x in aggregate_by:
            groupby_cols.extend(aggregateby_cols_dict[x])

        query = session.query(*groupby_cols, *value_cols).group_by(*groupby_cols)
        return query


    def roll_average(self, result, aggregate_by, rolling_days):

        if rolling_days is not None:
            date_column = None
            if aggregate_by is not None and "departure_date" in aggregate_by:
                date_column = "departure_date"
            if aggregate_by is not None and "arrival_date" in aggregate_by:
                date_column = "arrival_date"
            if date_column is None:
                logger.warning("No date to roll-average with. Not doing anything")
            else:
                min_date = result[date_column].min()
                max_date = result[date_column].max() # change your date here
                daterange = pd.date_range(min_date, max_date).rename(date_column)

                result[date_column] = result[date_column].dt.floor('D')  # Should have been done already
                result = result \
                    .groupby([x for x in result.columns if x not in [date_column, 'ship_dwt',
                                                                     'value_tonne', 'value_m3',
                                                                     'value_eur', 'value_currency', 'count']]) \
                    .apply(lambda x: x.set_index(date_column) \
                           .resample("D").sum() \
                           .reindex(daterange) \
                           .fillna(0) \
                           .rolling(rolling_days, min_periods=rolling_days) \
                           .mean()) \
                    .reset_index() \
                    .replace({np.nan: None})

        return result

    def pivot_result(self, result, pivot_by, pivot_value):

        dependencies = {
            'commodity': ['commodity_group', 'commodity_group_name'],
            'commodity_group': ['commodity', 'commodity_group_name'],
            'commodity_group_name': ['commodity', 'commodity_group'],

            'ship_insurer_country': ['ship_insurer_region', 'ship_insurer_iso2'],
            'ship_owner_country': ['ship_owner_region', 'ship_owner_iso2'],
            'ship_manager_country': ['ship_manager_region', 'ship_manager_iso2']
        }

        if pivot_by:
            pivot_by_dependencies = [d for x in to_list(pivot_by) for d in dependencies.get(x, [])]
            index = [x for x in result.columns
                     if not x.startswith('value') \
                     and not x in ['ship_dwt', 'count'] \
                     and x not in to_list(pivot_by)
                     and x not in pivot_by_dependencies]

            result = result.pivot_table(index=index,
                                        columns=to_list(pivot_by),
                                        values=pivot_value,
                                        sort=False,
                                        fill_value=0).reset_index()
            result['variable'] = pivot_value

        return result


    def spread_currencies(self, result):
        # We simply want to pivot across currencies
        # But pandas need clean non-null and hashable data, hence this whole function...
        len_before = len(result)
        n_currencies = len(result.currency.unique())
        sep = '#,#'
        old_columns = result.columns # Used to keep the order

        result['currency'] = 'value_' + result.currency.str.lower()

        # Create a hashable version
        if 'destination_names' in result.columns:
            result['destination_names'] = result['destination_names'].apply(lambda row: sep.join(row) if row else row)
            result['destination_iso2s'] = result['destination_iso2s'].apply(lambda row: sep.join([str(x) for x in row]) if row else row)
            result['destination_dates'] = result['destination_dates'].apply(
                lambda row: sep.join([x.strftime("%Y-%m-%d %H:%M:%S") for x in row]) if row else row)

        index_cols = [x for x in result.columns if x not in ['currency', 'value_currency', 'value_eur']]
        # result[index_cols] = result[index_cols].replace({np.nan: na_str})
        # result[index_cols] = result[index_cols].replace({None: na_str})

        result = result[index_cols + ['currency', 'value_currency']] \
            .set_index(index_cols + ['currency'])['value_currency'] \
            .unstack(-1).reset_index()

        # Recreate lists
        if 'destination_names' in result.columns:
            result.loc[~result.destination_names.isnull(), 'destination_names'] = \
                result.loc[~result.destination_names.isnull(), 'destination_names'].apply(lambda row: row.split(sep))

            result.loc[~result.destination_iso2s.isnull(), 'destination_iso2s'] = \
                result.loc[~result.destination_iso2s.isnull(), 'destination_iso2s'].apply(lambda row: row.split(sep))

            result.loc[~result.destination_dates.isnull(), 'destination_dates'] = \
                result.loc[~result.destination_dates.isnull(), 'destination_dates'].apply(lambda row: row.split(sep)) # We keep it as string

        # Quick sanity check
        len_after = len(result)
        assert len_after == len_before / n_currencies

        return result

    def select(self, result, select):

        if not select:
            return result

        names = []
        variables = []

        for s in to_list(select):
            m = re.match("(.*)\\((.*)\\)", s)
            if m:
                names.append(m[1])
                variables.append(m[2])
            else:
                # No asc(.*) or desc(.*)
                names.append(s)
                variables.append(s)

        result = result[variables]
        result.columns = names
        return result


    def sort_result(self, result, sort_by):
        by = []
        ascending = []
        default_ascending = False
        if sort_by:
            for s in sort_by:
                m = re.match("(.*)\\((.*)\\)", s)
                if m:
                    ascending.append(m[1] == "asc")
                    by.append(m[2])
                else:
                    # No asc(.*) or desc(.*)
                    ascending.append(default_ascending)
                    by.append(s)

            result.sort_values(by=by, ascending=ascending, inplace=True)

        return result


    def build_response(self, result, format, nest_in_data, aggregate_by, download, routed_trajectory=False):

        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=shipments.csv"})

        if format == "json":
            return Response(
                response=df_to_json(result, nest_in_data=nest_in_data),
                status=200,
                mimetype='application/json')

        if format in ["geojson", "kml"]:
            if aggregate_by is not None:
                return Response(
                    response="Cannot query geojson or kml with aggregation.",
                    status=HTTPStatus.BAD_REQUEST,
                    mimetype='application/json')

            shipment_ids = list([int(x) for x in result.id.unique()])

            trajectories = session.query(Trajectory) \
                .filter(Trajectory.shipment_id.in_(shipment_ids))

            trajectories_df = pd.read_sql(trajectories.statement, session.bind)

            if routed_trajectory:
                trajectories_df["geometry"] = trajectories_df.geometry_routed.combine_first(trajectories_df.geometry)

            trajectories_df.drop(["geometry_routed"], axis=1)
            trajectories_df = update_geometry_from_wkb(trajectories_df)

            result_gdf = gpd.GeoDataFrame(trajectories_df[["shipment_id", "geometry"]].rename(columns={'shipment_id': 'id'}),
                                          geometry='geometry') \
                            .merge(result)

            if format == 'geojson':
                result_geojson = result_gdf.to_json(cls=JsonEncoder)

                if nest_in_data:
                    resp_content = '{"data": ' + result_geojson + '}'
                else:
                    resp_content = result_geojson

                if download:
                    headers = {"Content-disposition":
                                   "attachment; filename=voyages.geojson"}
                else:
                    headers = {}

                return Response(
                    response=resp_content,
                    status=200,
                    mimetype='application/json',
                    headers=headers)

            if format == 'kml':
                import fiona
                import io
                fiona.supported_drivers['KML'] = 'rw'
                file_kml = io.BytesIO()

                result_gdf.to_file(file_kml, driver='KML')
                headers = {"Content-disposition": "attachment; filename=trajectories.kml"}
                file_kml.seek(0)
                return Response(
                    response=file_kml,
                    status=200,
                    mimetype='application/kml',
                    headers=headers)


        return Response(response="Unknown format. Should be either csv, json, geojson or kml",
                        status=HTTPStatus.BAD_REQUEST,
                        mimetype='application/json')