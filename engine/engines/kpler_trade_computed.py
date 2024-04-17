import numpy as np
from tqdm import tqdm
import pandas as pd
import datetime as dt

from sqlalchemy import (
    func,
    case,
    cast,
    nullslast,
    any_,
    true,
    String,
    Integer,
)
from sqlalchemy.orm import aliased
from sqlalchemy.dialects.postgresql import aggregate_order_by, array, ARRAY, array_agg
import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy import nullslast

from sqlalchemy.sql.expression import delete, insert

import base
from base.db import session, engine
from base.logger import logger_slack, logger
from engines.insurance_scraper import *
from base.models import (
    KplerProduct,
    Country,
    Price,
    Commodity,
    KplerTrade,
    KplerTradeComputed,
    KplerZone,
    Company,
    ShipInsurer,
    ShipOwner,
)

from base.models import DB_TABLE_KPLER_TRADE_COMPUTED

from base import UNKNOWN_INSURER


def string_array(values):
    return cast(array(values), ARRAY(String))


def integer_array(values):
    return cast(array(values), ARRAY(Integer))


def build_select(date_from: None, date_to: None):
    origin_zone = aliased(KplerZone)
    destination_zone = aliased(KplerZone)
    CommodityEquivalent = aliased(Commodity)

    price_date = func.date_trunc("day", KplerTrade.departure_date_utc)

    unnested_vessels = (
        func.unnest(KplerTrade.vessel_imos)
        .table_valued(
            "ship_imo", with_ordinality="ship_order", name="ships", joins_implicitly=False
        )
        .render_derived()
    )

    # This gives us the vessel IMOs as a table
    # to make other queries possible. We sort
    # to get the best performance.
    trade_ship = (
        session.query(
            KplerTrade.id.label("trade_id"),
            KplerTrade.flow_id,
            unnested_vessels.c.ship_order,
            unnested_vessels.c.ship_imo,
        )
        .select_from(KplerTrade)
        .join(unnested_vessels, true())
        .order_by(KplerTrade.id, KplerTrade.flow_id, unnested_vessels.c.ship_order)
        .filter(
            sa.and_(
                func.date_trunc("day", KplerTrade.departure_date_utc) >= date_from,
                func.date_trunc("day", KplerTrade.departure_date_utc) <= date_to,
                KplerTrade.is_valid == True,
            )
        )
        .cte("trade_ship")
        .prefix_with("MATERIALIZED")
    )

    # Commodity used for pricing
    commodity_id_field = build_commodity_id_field()

    pricing_commodity_id_field = case(
        [
            (
                sa.and_(
                    KplerProduct.group_name == "Crude/Co",
                    origin_zone.country_iso2 == "RU",
                    KplerProduct.grade_name.notin_(["CPC Kazakhstan", "KEBCO"]),
                    origin_zone.port_name.op("~*")("^Nakhodka|^De Kast|^Prigorod"),
                ),
                "crude_oil_espo",
            ),
            (
                sa.and_(
                    KplerProduct.group_name == "Crude/Co",
                    origin_zone.country_iso2 == "RU",
                    KplerProduct.grade_name.notin_(["CPC Kazakhstan", "KEBCO"]),
                ),
                "crude_oil_urals",
            ),
        ],
        else_=Commodity.pricing_commodity,
    ).label("pricing_commodity")

    ship_insurer_field = func.coalesce(ShipInsurer.date_from_insurer, ShipInsurer.date_from_equasis)

    insurance_buffer = sa.sql.expression.literal_column("INTERVAL '14 days'")

    # We get the latest (but starting before the
    # departure date) insurance per trade's ship.
    voyage_insurer = (
        session.query(
            KplerTrade.id.label("trade_id"),
            KplerTrade.flow_id,
            trade_ship.c.ship_imo,
            trade_ship.c.ship_order,
            func.coalesce(Company.name, "unknown").label("name"),
            func.coalesce(Company.country_iso2, "unknown").label("iso2"),
            func.coalesce(Country.region, "unknown").label("region"),
        )
        .join(
            trade_ship,
            sa.and_(
                KplerTrade.id == trade_ship.c.trade_id,
                KplerTrade.flow_id == trade_ship.c.flow_id,
            ),
        )
        .outerjoin(
            ShipInsurer,
            sa.and_(
                ShipInsurer.ship_imo == trade_ship.c.ship_imo,
                ShipInsurer.is_valid == True,
                sa.or_(
                    ship_insurer_field <= KplerTrade.departure_date_utc + insurance_buffer,
                    ship_insurer_field == None,
                ),
            ),
        )
        .outerjoin(Company, ShipInsurer.company_id == Company.id)
        .outerjoin(Country, Company.country_iso2 == Country.iso2)
        .distinct(KplerTrade.id, KplerTrade.flow_id, trade_ship.c.ship_imo)
        .order_by(
            KplerTrade.id,
            KplerTrade.flow_id,
            trade_ship.c.ship_imo,
            nullslast(ship_insurer_field.desc()),
        )
        .cte("voyage_insurer")
        .prefix_with("MATERIALIZED")
    )

    # We get the latest (but starting before the
    # departure date) owner per trade's ship.
    voyage_owner = (
        session.query(
            KplerTrade.id.label("trade_id"),
            KplerTrade.flow_id,
            trade_ship.c.ship_imo,
            trade_ship.c.ship_order,
            func.coalesce(Company.name, "unknown").label("name"),
            func.coalesce(Company.country_iso2, "unknown").label("iso2"),
            func.coalesce(Country.region, "unknown").label("region"),
        )
        .join(
            trade_ship,
            sa.and_(
                KplerTrade.id == trade_ship.c.trade_id,
                KplerTrade.flow_id == trade_ship.c.flow_id,
            ),
        )
        .outerjoin(
            ShipOwner,
            sa.and_(
                ShipOwner.ship_imo == trade_ship.c.ship_imo,
                sa.or_(
                    ShipOwner.date_from <= KplerTrade.departure_date_utc + insurance_buffer,
                    ShipOwner.date_from == None,
                ),
            ),
        )
        .outerjoin(Company, ShipOwner.company_id == Company.id)
        .outerjoin(Country, Company.country_iso2 == Country.iso2)
        .distinct(
            KplerTrade.id,
            KplerTrade.flow_id,
            trade_ship.c.ship_imo,
        )
        .order_by(
            KplerTrade.id,
            KplerTrade.flow_id,
            trade_ship.c.ship_imo,
            nullslast(ShipOwner.date_from.desc()),
        )
        .cte("voyage_owner")
        .prefix_with("MATERIALIZED")
    )

    # We need a separate query to get the best matching
    # price per ship per commodity. Afterwards we can choose the
    # one for each ship/commodity with the lowest price (the one
    # that is capped, if applicable).
    trade_ship_price = (
        session.query(
            KplerTrade.id.label("trade_id"),
            KplerTrade.flow_id,
            trade_ship.c.ship_imo,
            Price.scenario,
            Price.id.label("price_id"),
        )
        .join(
            trade_ship,
            sa.and_(
                KplerTrade.id == trade_ship.c.trade_id,
                KplerTrade.flow_id == trade_ship.c.flow_id,
            ),
        )
        .join(KplerProduct, KplerTrade.product_id == KplerProduct.id)
        .outerjoin(Commodity, commodity_id_field == Commodity.id)
        .outerjoin(origin_zone, KplerTrade.departure_zone_id == origin_zone.id)
        .outerjoin(destination_zone, KplerTrade.arrival_zone_id == destination_zone.id)
        .outerjoin(
            voyage_insurer,
            sa.and_(
                voyage_insurer.c.trade_id == KplerTrade.id,
                voyage_insurer.c.flow_id == KplerTrade.flow_id,
                voyage_insurer.c.ship_imo == trade_ship.c.ship_imo,
            ),
        )
        .outerjoin(
            voyage_owner,
            sa.and_(
                voyage_owner.c.trade_id == KplerTrade.id,
                voyage_owner.c.flow_id == KplerTrade.flow_id,
                voyage_owner.c.ship_imo == trade_ship.c.ship_imo,
            ),
        )
        .join(
            Price,
            sa.or_(
                # If it's from Russia, match it based on destination, insurer, or owener.
                sa.and_(
                    origin_zone.country_iso2 == "RU",
                    pricing_commodity_id_field == Price.commodity,
                    Price.date == price_date,
                    sa.or_(
                        voyage_insurer.c.iso2 == any_(Price.ship_insurer_iso2s),
                        Price.ship_insurer_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    sa.or_(
                        voyage_owner.c.iso2 == any_(Price.ship_owner_iso2s),
                        Price.ship_owner_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    sa.or_(
                        destination_zone.country_iso2 == any_(Price.destination_iso2s),
                        Price.destination_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    Price.departure_port_ids == base.PRICE_NULLARRAY_INT,
                ),
                # If it's not from Russia, we use the default pricing.
                sa.and_(
                    sa.or_(origin_zone.country_iso2 == None, origin_zone.country_iso2 != "RU"),
                    pricing_commodity_id_field == Price.commodity,
                    Price.date == price_date,
                    Price.ship_insurer_iso2s == base.PRICE_NULLARRAY_CHAR,
                    Price.ship_owner_iso2s == base.PRICE_NULLARRAY_CHAR,
                    Price.destination_iso2s == base.PRICE_NULLARRAY_CHAR,
                    Price.departure_port_ids == base.PRICE_NULLARRAY_INT,
                ),
            ),
        )
        .distinct(
            KplerTrade.id,
            KplerTrade.flow_id,
            trade_ship.c.ship_imo,
            Price.scenario,
        )
        .order_by(
            KplerTrade.id,
            KplerTrade.flow_id,
            trade_ship.c.ship_imo,
            Price.scenario,
            Price.destination_iso2s,
            Price.ship_insurer_iso2s,
            Price.ship_owner_iso2s,
        )
        .cte("trade_ship_price")
    )

    trade_price = (
        session.query(
            KplerTrade.id.label("trade_id"),
            KplerTrade.flow_id,
            Price.id.label("price_id"),
        )
        .join(
            trade_ship_price,
            sa.and_(
                KplerTrade.id == trade_ship_price.c.trade_id,
                KplerTrade.flow_id == trade_ship_price.c.flow_id,
            ),
        )
        .join(Price, trade_ship_price.c.price_id == Price.id)
        .distinct(
            KplerTrade.id,
            KplerTrade.flow_id,
            Price.scenario,
        )
        .order_by(
            KplerTrade.id,
            KplerTrade.flow_id,
            Price.scenario,
            nullslast(Price.eur_per_tonne),
        )
        .cte("trade_price")
    )

    g7 = base.G7_ISO2S

    insurers_and_owners_for_trade = (
        session.query(
            KplerTrade.id.label("trade_id"),
            KplerTrade.flow_id,
            # ownership
            array_agg(
                aggregate_order_by(
                    func.coalesce(voyage_owner.c.name, UNKNOWN_INSURER),
                    voyage_owner.c.ship_order,
                )
            ).label("ship_owner_names"),
            array_agg(aggregate_order_by(voyage_owner.c.iso2, voyage_owner.c.ship_order)).label(
                "ship_owner_iso2s"
            ),
            array_agg(aggregate_order_by(voyage_owner.c.region, voyage_owner.c.ship_order)).label(
                "ship_owner_regions"
            ),
            func.bool_or(sa.or_(voyage_owner.c.region == "EU", voyage_owner.c.iso2.in_(g7))).label(
                "owned_in_pcc"
            ),
            func.bool_or(voyage_owner.c.iso2 == "NO").label("owned_in_norway"),
            func.bool_or(voyage_owner.c.iso2 != None).label("owner_known"),
            # insurance
            array_agg(
                aggregate_order_by(
                    func.coalesce(voyage_insurer.c.name, UNKNOWN_INSURER),
                    voyage_insurer.c.ship_order,
                )
            ).label("ship_insurer_names"),
            array_agg(aggregate_order_by(voyage_insurer.c.iso2, voyage_insurer.c.ship_order)).label(
                "ship_insurer_iso2s"
            ),
            array_agg(
                aggregate_order_by(voyage_insurer.c.region, voyage_insurer.c.ship_order)
            ).label("ship_insurer_regions"),
            func.bool_or(
                sa.or_(voyage_insurer.c.region == "EU", voyage_insurer.c.iso2.in_(g7))
            ).label("insured_in_pcc"),
            func.bool_or(voyage_insurer.c.iso2 == "NO").label("insured_in_norway"),
        )
        .join(
            trade_ship,
            sa.and_(
                KplerTrade.id == trade_ship.c.trade_id,
                KplerTrade.flow_id == trade_ship.c.flow_id,
            ),
        )
        .outerjoin(
            voyage_owner,
            sa.and_(
                voyage_owner.c.trade_id == KplerTrade.id,
                voyage_owner.c.flow_id == KplerTrade.flow_id,
                voyage_owner.c.ship_imo == trade_ship.c.ship_imo,
            ),
        )
        .outerjoin(
            voyage_insurer,
            sa.and_(
                voyage_insurer.c.trade_id == KplerTrade.id,
                voyage_insurer.c.flow_id == KplerTrade.flow_id,
                voyage_insurer.c.ship_imo == trade_ship.c.ship_imo,
            ),
        )
        .group_by(KplerTrade.id, KplerTrade.flow_id)
        .order_by(KplerTrade.id, KplerTrade.flow_id)
        .cte("insurers_and_owners_for_trade")
    )

    ownership_sanction_coverage_field = case(
        (
            sa.or_(
                insurers_and_owners_for_trade.c.insured_in_pcc,
                insurers_and_owners_for_trade.c.owned_in_pcc,
            ),
            "Owned and / or insured in EU & G7",
        ),
        (
            insurers_and_owners_for_trade.c.insured_in_norway,
            "Insured in Norway",
        ),
        (
            insurers_and_owners_for_trade.c.owner_known,
            "Others",
        ),
        else_="Unknown",
    ).label("ownership_sanction_coverage")

    unnested_step_zones = (
        func.unnest(KplerTrade.step_zone_ids)
        .table_valued(
            "step_zone_id",
            with_ordinality="step_zone_order",
            name="step_zone",
            joins_implicitly=False,
        )
        .render_derived()
    )

    trade_step_zones = (
        session.query(
            KplerTrade.id.label("trade_id"),
            KplerTrade.flow_id,
            array_agg(
                aggregate_order_by(
                    KplerZone.name,
                    unnested_step_zones.c.step_zone_order,
                )
            ).label("step_zone_names"),
            array_agg(
                aggregate_order_by(
                    KplerZone.country_iso2,
                    unnested_step_zones.c.step_zone_order,
                )
            ).label("step_zone_iso2s"),
            array_agg(
                aggregate_order_by(Country.region, unnested_step_zones.c.step_zone_order)
            ).label("step_zone_regions"),
        )
        .select_from(KplerTrade)
        .join(unnested_step_zones, true())
        .outerjoin(KplerZone, unnested_step_zones.c.step_zone_id == KplerZone.id)
        .outerjoin(Country, KplerZone.country_iso2 == Country.iso2)
        .group_by(KplerTrade.id, KplerTrade.flow_id)
        .order_by(KplerTrade.id, KplerTrade.flow_id)
        .cte("trade_step_zones")
        .prefix_with("MATERIALIZED")
    )

    query = (
        session.query(
            # Renaming everything in terms of "origin" and "destination"
            KplerTrade.id.label("trade_id"),
            KplerTrade.flow_id,
            KplerTrade.product_id,
            # We only want to set this if there's a matching commodity
            Commodity.id.label("kpler_product_commodity_id"),
            Price.scenario.label("pricing_scenario"),
            Price.commodity.label("pricing_commodity"),
            Price.eur_per_tonne,
            insurers_and_owners_for_trade.c.ship_insurer_names,
            insurers_and_owners_for_trade.c.ship_insurer_iso2s,
            insurers_and_owners_for_trade.c.ship_insurer_regions,
            insurers_and_owners_for_trade.c.ship_owner_names,
            insurers_and_owners_for_trade.c.ship_owner_iso2s,
            insurers_and_owners_for_trade.c.ship_owner_regions,
            ownership_sanction_coverage_field,
            func.coalesce(trade_step_zones.c.step_zone_names, string_array([])).label(
                "step_zone_names"
            ),
            func.coalesce(trade_step_zones.c.step_zone_iso2s, string_array([])).label(
                "step_zone_iso2s"
            ),
            func.coalesce(trade_step_zones.c.step_zone_regions, string_array([])).label(
                "step_zone_regions"
            ),
            func.coalesce(KplerTrade.step_zone_ids, integer_array([])).label("step_zone_ids"),
        )
        .outerjoin(KplerProduct, KplerTrade.product_id == KplerProduct.id)
        .outerjoin(origin_zone, KplerTrade.departure_zone_id == origin_zone.id)
        .outerjoin(destination_zone, KplerTrade.arrival_zone_id == destination_zone.id)
        .outerjoin(Commodity, commodity_id_field == Commodity.id)
        .outerjoin(CommodityEquivalent, Commodity.equivalent_id == CommodityEquivalent.id)
        .outerjoin(
            trade_price,
            sa.and_(
                KplerTrade.id == trade_price.c.trade_id,
                KplerTrade.flow_id == trade_price.c.flow_id,
            ),
        )
        .outerjoin(
            Price,
            trade_price.c.price_id == Price.id,
        )
        .outerjoin(
            insurers_and_owners_for_trade,
            sa.and_(
                KplerTrade.id == insurers_and_owners_for_trade.c.trade_id,
                KplerTrade.flow_id == insurers_and_owners_for_trade.c.flow_id,
            ),
        )
        .outerjoin(
            trade_step_zones,
            sa.and_(
                KplerTrade.id == trade_step_zones.c.trade_id,
                KplerTrade.flow_id == trade_step_zones.c.flow_id,
            ),
        )
        .filter(
            sa.and_(
                func.date_trunc("day", KplerTrade.departure_date_utc) >= date_from,
                func.date_trunc("day", KplerTrade.departure_date_utc) <= date_to,
                KplerTrade.is_valid == True,
                Price.scenario != None,
            )
        )
        .order_by(KplerTrade.id, KplerTrade.flow_id, KplerTrade.product_id, Price.scenario)
    )

    return query.selectable


def build_pagination_periods(earliest_date=None, more_data_date=None):
    periods = list(
        map(
            lambda x: (
                (x - pd.offsets.MonthBegin(n=1)).date().isoformat(),
                x.date().isoformat(),
            ),
            list(
                pd.date_range(
                    more_data_date, end=dt.date.today() + pd.offsets.MonthEnd(n=1), freq="M"
                )
            ),
        )
    )
    periods.insert(
        0,
        (
            earliest_date.date().isoformat(),
            (more_data_date - dt.timedelta(days=1)).isoformat(),
        ),
    )
    periods.reverse()

    return periods


def update():
    logger_slack.info("=== Updating kpler computed table ===")
    try:
        earliest_date = session.query(func.min(KplerTrade.departure_date_utc)).first()[0]
        more_data_date = dt.date(2016, 1, 1)

        periods = build_pagination_periods(
            earliest_date=earliest_date, more_data_date=more_data_date
        )

        with session.begin_nested():
            session.execute(delete(KplerTradeComputed))
            for start, end in tqdm(periods, unit="period"):
                logger.info(f"Updating kpler computed table for {start} to {end}")
                session.execute(
                    insert(KplerTradeComputed).from_select(
                        [
                            "trade_id",
                            "flow_id",
                            "product_id",
                            "kpler_product_commodity_id",
                            "pricing_scenario",
                            "pricing_commodity",
                            "eur_per_tonne",
                            "ship_insurer_names",
                            "ship_insurer_iso2s",
                            "ship_insurer_regions",
                            "ship_owner_names",
                            "ship_owner_iso2s",
                            "ship_owner_regions",
                            "ownership_sanction_coverage",
                            "step_zone_names",
                            "step_zone_iso2s",
                            "step_zone_regions",
                            "step_zone_ids",
                        ],
                        build_select(date_from=start, date_to=end),
                    )
                )

            check_invalid_trade_computed()

        session.commit()

    except Exception as e:
        logger_slack.error(
            f"Updating kpler computed table failed",
            stack_info=True,
            exc_info=True,
        )


def check_invalid_trade_computed():
    """
    Inspect the computed trades that have no associated pricing
    and confirm that this is expected. Throw an error if not
    """
    ignorable_commodities = [
        "kpler_clean_condensate",
        "kpler_bitumen_asphalt",
        "kpler_cbfs",
        "kpler_coal_tar",
        "kpler_pitch",
        "kpler_specialities",
        "kpler_cutter_stock",
        "kpler_resids",
        "kpler_blendings",
    ]
    # Not all commodities have old pricing
    date_from = dt.datetime(2015, 1, 1)

    # Commodity used for pricing
    commodity_id_field = build_commodity_id_field()

    missing_trades = pd.DataFrame(
        session.query(
            KplerTrade.departure_date_utc,
            KplerTrade.id.label("trade_id"),
            KplerTrade.product_id,
            commodity_id_field.label("kpler_product_commodity_id"),
        )
        .join(KplerProduct, KplerTrade.product_id == KplerProduct.id)
        .outerjoin(
            KplerTradeComputed,
            (KplerTrade.id == KplerTradeComputed.trade_id)
            & (KplerTrade.product_id == KplerTradeComputed.product_id)
            & (KplerTrade.flow_id == KplerTradeComputed.flow_id),
        )
        .filter(
            sa.and_(
                KplerTradeComputed.trade_id == None,
                KplerTrade.is_valid == True,
                KplerTrade.product_id != None,
                commodity_id_field != None,
                commodity_id_field.notin_(ignorable_commodities),
                KplerTrade.departure_date_utc >= date_from,
            )
        )
        .all()
    )

    if any(missing_trades):
        logger_slack.error(f"Computed trades without pricing found")
        raise Exception(f"Computed trades without pricing found:\n{missing_trades}")


def build_commodity_id_field():
    return "kpler_" + sa.func.replace(
        sa.func.replace(
            sa.func.lower(func.coalesce(KplerProduct.commodity_name, KplerProduct.group_name)),
            " ",
            "_",
        ),
        "/",
        "_",
    )
