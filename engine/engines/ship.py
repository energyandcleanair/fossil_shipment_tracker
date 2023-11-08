from tqdm import tqdm
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
import base
import json

from base.db import session
from base.logger import logger, logger_slack
from base.models import (
    Ship,
    PortCall,
    Departure,
    Shipment,
    ShipmentDepartureBerth,
    Trajectory,
    MTVoyageInfo,
    Arrival,
)
from base.utils import to_datetime, to_list
from engines.datalastic import default_datalastic
import numpy as np

import sqlalchemy as sa


def update():
    # Not much really. We just confirm crude_oil vs oil_products when necessary
    # And use MT for insurance
    logger_slack.info("=== Updating ships ===")
    collect_mt_for_large_oil_products()
    # collect_mt_for_insurers()
    return


def collect_mt_for_large_oil_products():
    """
    Datalastic indicates Oil Products Tanker for certain tankers
    that are marked as Crude Oil tankers by MT.
    We trust MT, and recollect MT for 'dubious' ships
    :return:
    """

    from engines.marinetraffic import Marinetraffic

    ships = Ship.query.filter(
        Ship.commodity == base.OIL_PRODUCTS,
        Ship.dwt >= 40e3,
    ).all()

    for ship in tqdm(ships, unit="ship"):
        if ship.type != ship.others.get("marinetraffic", {}).get("VESSEL_TYPE"):
            # If there are multiple mmsis take latest one
            ship_mt = Marinetraffic.get_ship(mmsi=ship.mmsi[-1])
            if ship_mt is not None and ship.imo == ship_mt.imo:
                ship_mt.others.update(ship.others)
                (commodity, quantity, unit) = ship_to_commodity(ship_mt)
                ship_mt.commodity = commodity
                ship_mt.quantity = quantity
                ship_mt.unit = unit
                ship_mt.subtype = None
                session.merge(ship_mt)
                session.commit()
            else:
                logger.info("IMOs don't match or ship not found")
        else:
            logger.info("Was already using MT")


# def collect_mt_for_insurers(date_from='2022-02-24',
#                          commodity=[base.CRUDE_OIL, base.LNG]):
#
#     ships = session.query(Ship) \
#         .join(Departure, Ship.imo == Departure.ship_imo) \
#         .join(Shipment, Shipment.departure_id == Departure.id) \
#         .filter(Departure.date_utc >= date_from) \
#         .filter(Ship.insurer == sa.null()) \
#         .filter(Ship.commodity.in_(to_list(commodity))) \
#         .distinct() \
#         .all()
#
#     for ship in tqdm(ships):
#         if not 'INSURER' in ship.others.get('marinetraffic', {}).keys():
#             ship_mt = Marinetraffic.get_ship(mmsi=ship.mmsi, use_cache=True)
#             # ship_mt = Marinetraffic.get_ship(imo=ship.imo, use_cache=False)
#             if ship_mt is not None and ship.imo == ship_mt.imo:
#                 if 'datalastic' in ship.others:
#                     ship_mt.others['datalastic'] = ship.others['datalastic']
#                 session.merge(ship_mt)
#                 session.commit()
#             else:
#                 logger.info("IMOs don't match or ship not found")


def fill_missing_commodity():
    # First fill type if missing
    # Datalastic (or us) seem to have been missing a few in the past
    ships = Ship.query.filter(Ship.type == sa.null()).all()
    for ship in tqdm(ships, unit="ship"):
        new_ship = default_datalastic.get_ship(imo=ship.imo, use_cache=False)
        if new_ship:
            logger.info("Found new ship {new_ship.imo}")
            ship.others.update({"datalastic": new_ship.others.get("datalastic")})
            new_ship.others.update(ship.others)
            # new_ship.others.update(ship.others)
            (commodity, quantity, unit) = ship_to_commodity(new_ship)
            new_ship.commodity = commodity
            new_ship.quantity = quantity
            new_ship.unit = unit
            session.merge(new_ship)
            session.commit()

    ships = Ship.query.filter(Ship.commodity == sa.null()).all()
    for ship in tqdm(ships, unit="ship"):
        (commodity, quantity, unit) = ship_to_commodity(ship)
        ship.commodity = commodity
        ship.quantity = quantity
        ship.unit = unit
        session.commit()


def fill(imos=[], mmsis=[], force=False):
    """
    Fill database (append or upsert) ship information
    :param imos: list of imos codes
    :param source: what data source to use
    :return:
    """
    imos = list(set([str(x) for x in imos]))
    mmsis = list(set([str(x) for x in mmsis]))

    # Fill missing ships
    def get_missing_ships_imos(imos):
        existing_imos = [value for value, in session.query(Ship.imo).all()]
        return [x for x in imos if (str(x) not in existing_imos or force)]

    def get_missing_ships_mmsis(mmsis):
        existing_mmsis = [x for x, in session.query(func.unnest(Ship.mmsi)).all()]
        return [x for x in mmsis if (str(x) not in existing_mmsis or force)]

    if not get_missing_ships_imos(imos) and not get_missing_ships_mmsis(mmsis):
        # Ship already in db
        return True

    logger.info("Adding %d missing ships" % (len(imos) + len(mmsis)))

    # First with Datalastic - we do check if Datalastic found the ship properly by checking dwt, and refer to
    # MT to retry if it did not
    ships = [
        default_datalastic.get_ship(imo=x, query_if_not_in_cache=True)
        for x in get_missing_ships_imos(imos)
    ]
    upload_ships([s for s in ships if (s and s.dwt is not None and s.type is not None)])

    ships = [
        default_datalastic.get_ship(mmsi=x, query_if_not_in_cache=True)
        for x in get_missing_ships_mmsis(mmsis)
    ]
    upload_ships([s for s in ships if (s and s.dwt is not None and s.type is not None)])

    # Then with Marinetraffic for those still missing
    from engines.marinetraffic import Marinetraffic

    ships = [Marinetraffic.get_ship(imo=x) for x in get_missing_ships_imos(imos)]
    upload_ships(ships)

    ships = [Marinetraffic.get_ship(mmsi=x) for x in get_missing_ships_mmsis(mmsis)]
    upload_ships(ships)

    missing = get_missing_ships_imos(imos)
    missing.extend(get_missing_ships_mmsis(mmsis))
    if missing:
        logger.warning("Some ships are still missing: %s" % (",".join(missing)))
        return False

    return True


def upload_ships(ships):
    for ship in ships:
        if ship and ship.imo is not None:
            ship = set_commodity(ship)
            session.add(ship)
        try:
            session.commit()
        except sa.exc.IntegrityError as e:
            session.rollback()
            # Ship with this IMO probably already exists.
            imo_ships = session.query(Ship).filter(Ship.imo.op("~")(ship.imo)).all()

            if len(imo_ships) > 1:
                logger.warning(
                    "Please check ship imo {}, we have more than one ship in db.".format(ship.imo)
                )
            if len(imo_ships) == 1:
                # add new shop mmsi to existing ship imo
                imo_ship, mmsis = imo_ships[0], imo_ships[0].mmsi
                if ship.mmsi[-1] not in mmsis:
                    imo_ship.mmsi = imo_ship.mmsi + ship.mmsi
                    session.commit()
            else:
                raise ValueError("Problem inserting ship: %s" % (str(e),))


def ship_to_commodity(ship):
    """
    Guess commodity, and quantity of ship
    :param ship:
    :return: [commodity, quantity, unit]
    """
    import re

    try:
        type = ship.type or ""
        subtype = ship.subtype or ""
        if re.match("Crude oil", type, re.IGNORECASE) or re.match(
            "Crude oil", subtype, re.IGNORECASE
        ):
            commodity = base.CRUDE_OIL
        elif re.match("OIL/CHEMICAL", type, re.IGNORECASE) or re.match(
            "Oil or chemical", subtype, re.IGNORECASE
        ):
            commodity = base.OIL_OR_CHEMICAL
        elif re.match("OIL PRODUCTS", type, re.IGNORECASE) or re.match(
            "Oil products", subtype, re.IGNORECASE
        ):
            commodity = base.OIL_PRODUCTS
        elif re.match("LNG", type, re.IGNORECASE) or re.match("LNG", subtype, re.IGNORECASE):
            commodity = base.LNG
        elif re.match("LPG", type, re.IGNORECASE) or re.match("LPG", subtype, re.IGNORECASE):
            commodity = base.LPG
        elif re.match("Ore or Oil", subtype, re.IGNORECASE):
            commodity = base.OIL_OR_ORE
        elif re.match("Bulk", type, re.IGNORECASE) or re.match("Bulk", subtype, re.IGNORECASE):
            commodity = base.BULK
        elif re.search("cargo", type, re.IGNORECASE):
            commodity = base.GENERAL_CARGO
        else:
            commodity = base.UNKNOWN_COMMODITY

    except TypeError:
        commodity = base.UNKNOWN_COMMODITY

    if ship.liquid_gas is not None and commodity == base.LNG:
        unit = "m3"
        quantity = ship.liquid_gas
    # TODO Should we consider the m3 information for oil?
    else:
        unit = "tonne"
        quantity = ship.dwt

    # # Heuristic1: if oil_products but dwt > 90,000t, then assume crude_oil
    if ship.dwt and float(ship.dwt) > 90e3 and commodity in [base.OIL_PRODUCTS]:
        commodity = base.CRUDE_OIL

    return [commodity, quantity, unit]


def set_commodity(ship):
    """

    Parameters
    ----------
    ship :

    Returns
    -------

    """
    [commodity, quantity, unit] = ship_to_commodity(ship)
    ship.commodity = commodity
    ship.quantity = quantity
    ship.unit = unit
    return ship


def fix_duplicate_imo(imo=None, handle_versioned=True, handle_not_found=True):
    """
    Verify the ship table - check if there are any identical rows - this could happen with the historic version
    of using '_v*' to iterate ships

    Parameters
    ----------
    imo :
    handle_versioned:
    handle_not_found:

    Returns
    -------

    """
    from engines.marinetraffic import Marinetraffic

    def return_coalesced_data(ships):
        """
        Combine multiple ship object other data columns into one based on content

        Parameters
        ----------
        ships :

        Returns
        -------

        """

        if not ships:
            return None

        def return_all_values(d):
            for v in d.values():
                if isinstance(v, dict):
                    yield from return_all_values(v)
                else:
                    if v is not None and v != "":
                        yield v

        # collapse all sources into lists, eg equasis, datalastic, marinetraffic
        sources = {}
        for ship in ships:
            if not ship.others:
                continue

            for source, data in ship.others.items():
                sources[source] = sources.get(source, []) + [data]

        # sort all data
        for source, data in sources.items():
            sources[source] = sorted(
                data, key=lambda item: len(list(return_all_values(item))), reverse=True
            )[0]

        return sources

    def update_ship_imo(old_imo, new_imo):
        """
        Correct any existing departures/portcalls with new imo

        Parameters
        ----------
        old_imo :
        new_imo :

        Returns
        -------

        """

        ship_portcalls = PortCall.query.filter(PortCall.ship_imo == old_imo).all()
        ship_mtvoyages = MTVoyageInfo.query.filter(MTVoyageInfo.ship_imo == old_imo).all()

        # First let's try and clean up mtvoyage which should not have issues with changes - it is just to make sure
        # mmsi arrays are consistent everywhere
        for ship_mtvoyage in ship_mtvoyages:
            ship_mtvoyage.ship_imo = new_imo
            try:
                session.commit()
            except sa.exc.IntegrityError as e:
                session.rollback()
                logger.info(
                    "Duplicate voyageinfo {} - failed to delete. Please check.".format(
                        ship_mtvoyage.id
                    )
                )

        for ship_portcall in ship_portcalls:
            ship_portcall.ship_imo = new_imo
            try:
                session.commit()
            except sa.exc.IntegrityError as e:
                session.rollback()
                logger.info(
                    "Duplicate portcall_id {} - deleting portcall, departure and associated shipments.".format(
                        ship_portcall.id
                    )
                )

                # find associated shipments to delete
                shipments_to_delete = (
                    session.query(Shipment)
                    .join(Departure, Departure.id == Shipment.departure_id)
                    .filter(Departure.portcall_id == ship_portcall.id)
                )

                shipments_to_delete_list = [s.id for s in shipments_to_delete.all()]

                # delete in correct order to respect foreign keys
                session.query(Shipment).filter(Shipment.id.in_(shipments_to_delete_list)).delete()
                session.query(Departure).filter(Departure.portcall_id == ship_portcall.id).delete()

                session.query(ShipmentDepartureBerth).filter(
                    ShipmentDepartureBerth.shipment_id.in_(shipments_to_delete_list)
                ).delete()
                session.query(Trajectory).filter(
                    Trajectory.shipment_id.in_(shipments_to_delete_list)
                ).delete()

                session.delete(ship_portcall)

                # try and commit and if it fails undo
                try:
                    session.commit()
                except sa.exc.IntegrityError:
                    logger.info(
                        "Failed to delete portcall_id {} and associated objects.".format(
                            ship_portcall.id
                        )
                    )
                    session.rollback()
                    continue

        ship_departures = Departure.query.filter(Departure.ship_imo == old_imo).all()
        for ship_departure in ship_departures:
            ship_departure.ship_imo = new_imo

        try:
            session.commit()
        except sa.exc.IntegrityError as e:
            logger.info("Failed to create update ship IMO", stack_info=True, exc_info=True)
            session.rollback()

    ships = (
        session.query(
            sa.case(
                [
                    (Ship.imo.like("%_v%"), sa.func.split_part(Ship.imo, "_", 1)),
                    (Ship.imo.like("%-%"), sa.func.split_part(Ship.imo, "-", 1)),
                ],
                else_=sa.null(),
            ).label("imo"),
            sa.case(
                [(Ship.imo.like("%NOTFOUND%"), sa.func.split_part(Ship.imo, "_", 2))],
                else_=sa.null(),
            ).label("mmsi"),
            Ship.imo.label("old_imo"),
        )
        .filter(sa.or_(Ship.imo.op("~")("[_v]"), Ship.imo.op("~")("[NOTFOUND]")))
        .subquery()
    )

    ships = session.query(ships).distinct(ships.c.imo, ships.c.mmsi).subquery()

    if imo:
        ships = (
            session.query(ships)
            .filter(sa.or_(ships.c.imo.in_(to_list(imo)), ships.c.old_imo.in_(to_list(imo))))
            .subquery()
        )

    if not handle_not_found:
        ships = session.query(ships).filter(~ships.c.old_imo.op("~")("NOTFOUND"))

    if not handle_versioned:
        ships = session.query(ships).filter(~ships.c.old_imo.op("~")("_v"))

    ships = ships.all()

    for ship in tqdm(ships, unit="ship"):
        logger.info("Checking vessel imo: {}, mmsi: {}.".format(ship.imo, ship.mmsi))

        if "NOTFOUND" in ship.old_imo and not handle_not_found:
            continue

        base_imo = ship.imo

        if base_imo is not None and "NOTFOUND" not in ship.old_imo:
            ship_versions = session.query(Ship).filter(Ship.imo.op("~")(base_imo)).all()

            # Check if we only have 1 version - this should not happen as we select distinct
            if len(ship_versions) == 1:
                continue

            # However, it is easier to keep the non-version ship object so we do not have to change events/portcalls
            ship_versions = sorted(
                ship_versions, key=lambda item: len(str(item.imo)), reverse=False
            )

            ship_to_keep = ship_versions[0]

            # If we only have ships with _v versioning and no base ship, let's skip and check manually
            if "_v" in ship_to_keep.imo:
                logger.info(
                    "Found a ship with no non-version object, ship imo: {}. Please check.".format(
                        ship_to_keep.imo
                    )
                )
                continue

            # combine other data column to store for the future
            other_data = return_coalesced_data(ship_versions)
            mmsis = list(set([mmsi for s in ship_versions for mmsi in s.mmsi]))
            names = list(set([name for s in ship_versions for name in s.name]))

            # check if existing versions of ships have the same dwt/name/mmsi - in which case we can simplify
            if (
                len(set([s.dwt for s in ship_versions])) == 1
                and len(names) == 1
                and len(mmsis) == 1
            ):
                old_imo = ship_to_keep.imo

                # fix the rest of the ship versions by changing departures/portcalls and deleting them after
                for sv in ship_versions[1:]:
                    # fix departures/portcalls if necessary
                    update_ship_imo(sv.imo, old_imo)

                    # remove ship
                    session.delete(sv)

                # we run flush to make sure the change is reflected, as we need to update ship we're keeping to existing
                # imo
                try:
                    session.flush()
                except sa.exc.IntegrityError:
                    session.rollback()
                    continue

                # update the object we're keeping with all info we want to contain and collapse mmsis into list
                ship_to_keep.imo = base_imo
                ship_to_keep.others = other_data
                ship_to_keep.mmsi = mmsis
                ship_to_keep.name = names

                try:
                    session.commit()
                except sa.exc.IntegrityError:
                    session.rollback()
                    logger.info("Failed to fix ship imo {}.".format(base_imo))

        else:
            # first let's try and deal with NOTFOUND cases
            if base_imo is None:
                if fill(mmsis=[ship.mmsi], force=True):
                    logger.info("Found NOTFOUND ship (mmsi: {}).".format(ship.mmsi))

                    found_ship = session.query(Ship).filter(Ship.mmsi.any(ship.mmsi)).first()

                    update_ship_imo(ship.old_imo, found_ship.imo)

                else:
                    logger.warning(
                        "Could not find NOTFOUND ship (mmsi: {}). Trying with MT id.".format(
                            ship.mmsi
                        )
                    )

                    portcall = PortCall.query.filter(PortCall.ship_imo == ship.old_imo).first()
                    if portcall:
                        ship_mt_id = portcall.others.get("marinetraffic", {}).get("SHIP_ID")
                        new_ship = Marinetraffic.get_ship(mt_id=ship_mt_id, use_cache=False)

                        # Add existing ship if not in db
                        if new_ship:
                            existing_ship = Ship.query.filter(
                                sa.and_(
                                    Ship.imo == new_ship.imo,
                                    sa.or_(
                                        Ship.mmsi.any(new_ship.mmsi[-1]),
                                        Ship.name.any(new_ship.name[-1]),
                                        Ship.dwt == new_ship.dwt,
                                    ),
                                )
                            ).first()

                            if not existing_ship and new_ship.imo is not None:
                                try:
                                    session.add(new_ship)
                                    session.commit()

                                    update_ship_imo(ship.old_imo, new_ship.imo)

                                    Ship.query.filter(Ship.imo == ship.old_imo).delete()
                                    session.commit()
                                except IntegrityError:
                                    session.rollback()  # Do manual edit here for now

                            if new_ship and new_ship.name == ship.name:
                                update_ship_imo(ship.old_imo, new_ship.imo)

                                Ship.query.filter(Ship.imo == ship.old_imo).delete()
                                session.commit()
                            else:
                                logger.info(
                                    "%s \n vs. \n %s " % (str(new_ship.others), str(ship.others))
                                )

            else:
                # we have conflicting information for a minotiry of cases -
                # we fill ship using imo to get the latest data and make sure dwt is correct
                found_ship = default_datalastic.get_ship(imo=base_imo)
                if found_ship is None or found_ship.dwt is None:
                    found_ship = Marinetraffic.get_ship(imo=base_imo)

                if found_ship is not None:
                    found_mmsi, found_name = found_ship.mmsi[0], found_ship.name[0]
                    # add or over ride existing others data with newest
                    for source, data in found_ship.others.items():
                        other_data[source] = data
                    if found_mmsi not in mmsis:
                        mmsis.append(found_mmsi)
                    if found_name not in names:
                        names.append(found_name)

                    # Ugly, but we enforce the latest mmsi/name is now at the end of the list
                    names.append(names.pop(names.index(found_name)))
                    mmsis.append(mmsis.pop(mmsis.index(found_mmsi)))

                    # Update existing ship with new data
                    ship_to_keep.others = other_data
                    ship_to_keep.mmsi = mmsis
                    ship_to_keep.name = names
                    ship_to_keep.dwt = found_ship.dwt
                    ship_to_keep.type = found_ship.type
                    ship_to_keep.subtype = found_ship.subtype

                    ship_to_keep = set_commodity(ship_to_keep)

                    for sv in ship_versions[1:]:
                        update_ship_imo(sv.imo, base_imo)
                        session.delete(sv)

                    try:
                        session.commit()
                    except sa.exc.IntegrityError:
                        session.rollback()

                else:
                    logger.info(
                        "Failed to update ship_imo {}, we will use existing ship object.".format(
                            base_imo
                        )
                    )
                    for sv in ship_versions:
                        if "_v" in sv.imo:
                            update_ship_imo(sv.imo, base_imo)
                            session.delete(sv)
                    try:
                        session.commit()
                    except sa.exc.IntegrityError:
                        logger.info(
                            "Failed update existing portcalls/departures for ship imo {}.".format(
                                base_imo
                            )
                        )
                        session.rollback()


def fix_mmsi_imo_discrepancy(date_from=None):
    """

    Parameters
    ----------
    date_from :
    """
    query = session.query(PortCall.ship_imo, PortCall.ship_mmsi)
    if date_from is not None:
        query = query.filter(PortCall.date_utc >= to_datetime(date_from))

    portcall_ships = query.distinct().all()

    correct = []
    wrong = []
    unknown = []
    # For each ship, ask datalastic
    for portcall_ship in tqdm(portcall_ships, unit="portcall-ship"):
        imo = portcall_ship.ship_imo
        mmsi = portcall_ship.ship_mmsi[-1]
        # others = portcall_ship.others
        found_ship = default_datalastic.get_ship(mmsi=mmsi, use_cache=True)
        if not found_ship or not found_ship.imo or found_ship.imo == "0":
            from engines.marinetraffic import Marinetraffic

            found_ship = Marinetraffic.get_ship(mmsi=mmsi, use_cache=True)

        if not found_ship:
            unknown.append(mmsi)
        elif found_ship.imo == imo:
            correct.append(mmsi)
        else:
            correct_imo = found_ship.imo
            existing_ship = Ship.query.filter(Ship.imo == correct_imo).all()
            if len(existing_ship) > 1:
                logger.warning("Found more than one")
                continue

            if len(existing_ship) == 0:
                if not found_ship.imo:
                    n_imo_ships = Ship.query.filter(Ship.imo.op("~")("unknown" + "[_v]?")).count()
                    new_imo = "%s_v%d" % ("unknown", n_imo_ships + 1)
                    found_ship.imo = new_imo
                session.add(found_ship)
                session.commit()

                ship_portcalls = PortCall.query.filter(PortCall.ship_mmsi == mmsi).all()
                for ship_portcall in ship_portcalls:
                    ship_portcall.ship_imo = correct_imo

                ship_departures = Departure.query.filter(Departure.ship_imo == imo).all()
                for ship_departure in ship_departures:
                    ship_departure.ship_imo = correct_imo

                try:
                    session.commit()
                except sa.exc.IntegrityError as e:
                    logger.info(
                        f"Failed to fix imo/mmsi integrity for {imo}/{mmsi}",
                        stack_info=True,
                        exc_info=True,
                    )
                    session.rollback()

            if len(existing_ship) == 1:
                if existing_ship[0].mmsi == mmsi:
                    # Just need to plug with existing ship
                    ship_portcalls = PortCall.query.filter(PortCall.ship_mmsi == mmsi).all()
                    for ship_portcall in ship_portcalls:
                        ship_portcall.ship_imo = correct_imo

                    ship_departures = Departure.query.filter(Departure.ship_imo == imo).all()
                    for ship_departure in ship_departures:
                        ship_departure.ship_imo = correct_imo

                    try:
                        session.commit()
                    except sa.exc.IntegrityError as e:
                        logger.info(
                            f"Failed to fix imo/mmsi integrity for {imo}/{mmsi}",
                            stack_info=True,
                            exc_info=True,
                        )
                        session.rollback()

                else:
                    n_imo_ships = Ship.query.filter(Ship.imo.op("~")(correct_imo + "[_v]?")).count()
                    new_imo = "%s_v%d" % (correct_imo, n_imo_ships + 1)
                    found_ship.imo = new_imo
                    session.add(found_ship)
                    session.commit()

                    ship_portcalls = PortCall.query.filter(PortCall.ship_mmsi == mmsi).all()
                    for ship_portcall in ship_portcalls:
                        ship_portcall.ship_imo = new_imo

                    ship_departures = Departure.query.filter(Departure.ship_imo == imo).all()
                    for ship_departure in ship_departures:
                        ship_departure.ship_imo = new_imo

                    try:
                        session.commit()
                    except sa.exc.IntegrityError as e:
                        logger.info(
                            f"Failed to fix imo/mmsi integrity for {imo}/{mmsi}",
                            stack_info=True,
                            exc_info=True,
                        )
                        session.rollback()

            wrong.append(mmsi)

        # # Also update Others in portcall
        # if others and found_ship:
        #     others_imo = others.get("marinetraffic",{}).get("IMO")
        #     if others_imo and others_imo != found_ship.imo:
        #         print(2)
    logger.info(f"=== Correct: {len(correct)} | Wrong: {len(wrong)} | Unknown: {len(unknown)} ===")


def fix_not_found():
    ships = Ship.query.filter(Ship.imo.op("~*")("NOTFOUND.*|.*_v.*")).all()

    from engines.marinetraffic import Marinetraffic

    # portcalls = PortCall.query.filter(PortCall.ship_imo.op('~*')('NOTFOUND.*')).all()

    for ship in tqdm(ships, unit="ship"):
        portcall = PortCall.query.filter(PortCall.ship_imo == ship.imo).first()
        if portcall:
            ship_mt_id = portcall.others.get("marinetraffic", {}).get("SHIP_ID")
            new_ship = Marinetraffic.get_ship(mt_id=ship_mt_id, use_cache=True)

            # Add existing ship if not in db
            if new_ship:
                existing_ship = Ship.query.filter(
                    sa.and_(
                        Ship.imo == new_ship.imo,
                        sa.or_(
                            Ship.mmsi == new_ship.mmsi,
                            Ship.name == new_ship.name,
                            Ship.dwt == new_ship.dwt,
                        ),
                    )
                ).first()

                if not existing_ship and new_ship.imo is not None:
                    try:
                        session.add(new_ship)
                        session.commit()
                    except IntegrityError:
                        session.rollback()  # Do manual edit here for now

                if new_ship and new_ship.name == ship.name:
                    try:
                        session.query(PortCall).filter(PortCall.ship_imo == ship.imo).update(
                            {"ship_imo": new_ship.imo}
                        )
                        session.commit()
                    except IntegrityError as e:
                        session.rollback()  # Do manual delete here for now

                    session.query(Departure).filter(Departure.ship_imo == ship.imo).update(
                        {"ship_imo": new_ship.imo}
                    )
                    session.commit()

                    Ship.query.filter(Ship.imo == ship.imo).delete()
                    session.commit()
                else:
                    # What to do?
                    logger.info("%s \n vs. \n %s " % (str(new_ship.others), str(ship.others)))


def compare_ship_sources(
    dwt_min=None,
    sample=None,
    limit=None,
    reload_marinetraffic=False,
    commodity=[base.CRUDE_OIL],
):
    """

    :return:
    """

    from engines.marinetraffic import Marinetraffic

    # Let's use a simplified estimate to find out what our biggest transporters of commodities are
    largest_transporters = (
        session.query(
            Ship,
            sa.func.avg(Ship.dwt).label("dwt"),
            sa.func.sum(Ship.dwt).label("total_dwt"),
            sa.func.count(Shipment.id.label("shipment_id")).label("n_shipments"),
        )
        .join(Departure, Departure.id == Shipment.departure_id)
        .join(Arrival, Arrival.id == Shipment.arrival_id)
        .join(Ship, Ship.imo == Departure.ship_imo)
        .filter(Ship.others.has_key("marinetraffic"), ~Ship.others.has_key("datalastic"))
        .group_by(Ship.imo)
        .order_by(sa.func.sum(Ship.dwt).label("total_dwt").desc())
    )

    if dwt_min:
        largest_transporters = largest_transporters.filter(Ship.dwt > dwt_min)

    if commodity:
        largest_transporters = largest_transporters.filter(Ship.commodity.in_(to_list(commodity)))

    largest_transporters = largest_transporters.all()

    if limit:
        largest_transporters = largest_transporters[0:limit]

    if sample:
        largest_transporters = [
            largest_transporters[i]
            for i in np.random.choice(len(largest_transporters), sample, replace=True)
        ]

    matching = []

    for ship in tqdm(largest_transporters, unit="large-ship"):
        ship_mt = ship[0]
        if reload_marinetraffic:
            ship_mt = Marinetraffic.get_ship(imo=ship_mt.imo)

        ship_dt = set_commodity(default_datalastic.get_ship(imo=ship_mt.imo))

        if (ship_mt.dwt == ship_dt.dwt) & (ship_mt.commodity == ship_dt.commodity):
            if ship_mt.name != ship_dt.name:
                logger.info(
                    "Ship imo {}, names do not match, but dwt and commodity do.".format(ship_mt.imo)
                )
            matching.append(ship_dt)
        else:
            logger.info(
                "Ship imo {}, dwt/commodity did not match. DWT: {}/{}, Name: {}/{}, Commodity: {}/{}".format(
                    ship_mt.imo,
                    ship_mt.dwt,
                    ship_dt.dwt,
                    ship_mt.name,
                    ship_dt.name,
                    ship_mt.commodity,
                    ship_dt.commodity,
                )
            )

    logger.info("Ships are identical for {}% of cases.".format(100 * len(matching) / float(sample)))
