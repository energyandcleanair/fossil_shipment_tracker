import datetime as dt
import logging
import re
from typing import TypedDict

from sqlalchemy import func, tablesample, nulls_last, case
from sqlalchemy.orm import aliased

from base.db import session
from base.models.kpler import (
    KplerTrade,
    KplerTradeComputed,
    KplerProduct,
    KplerZone,
    KplerVessel,
)

import sqlalchemy as sa

from base.models import Commodity, ShipInsurer, ShipOwner, Company, Price, ShipFlag

from base.logger import logger
from tqdm import tqdm

from tqdm.contrib.logging import logging_redirect_tqdm


def test_sample_computed():
    computed_sampled = aliased(
        KplerTradeComputed, tablesample(KplerTradeComputed, func.bernoulli(1))
    )

    sampled_computed: list[KplerTradeComputed] = session.query(computed_sampled).limit(1000).all()

    errors = []
    with logging_redirect_tqdm(loggers=[logging.root]):
        for sample in tqdm(sampled_computed):
            try:
                test_sample(sample)
            except AssertionError as e:
                errors.append(e)
    if errors:
        all_errors = "\n".join([str(error) for error in errors])
        raise AssertionError(f"Errors found in {len(errors)} samples: {all_errors}")


def test_sample(sample: KplerTradeComputed):

    sample_id = f"id{sample.trade_id}_flow{sample.flow_id}"

    trade: KplerTrade = (
        session.query(KplerTrade)
        .filter(
            KplerTrade.id == sample.trade_id,
            KplerTrade.flow_id == sample.flow_id,
            KplerTrade.is_valid,
        )
        .first()
    )
    assert trade, f"Trade {sample.trade_id} not found for sample {sample_id}"
    assert trade.is_valid, f"Trade {sample.trade_id} is not valid for sample {sample_id}"
    product: KplerProduct = (
        session.query(KplerProduct).filter(KplerProduct.id == trade.product_id).first()
    )

    expected_commodity_id = extract_expected_commodity_id(product)

    assert (
        sample.kpler_product_commodity_id == expected_commodity_id
    ), f"Computed product id is wrong for sample {sample_id}: expected {expected_commodity_id} but got {sample.kpler_product_commodity_id}"

    commodity: Commodity = (
        session.query(Commodity).filter(Commodity.id == expected_commodity_id).first()
    )

    departure_zone: KplerZone = (
        session.query(KplerZone).filter(KplerZone.id == trade.departure_zone_id).first()
    )

    expected_commodity = extract_expected_pricing_commodity(product, commodity, departure_zone)

    assert (
        sample.pricing_commodity == expected_commodity
    ), f"Computed pricing commodity is wrong for sample {sample_id}: expected {expected_commodity} but got {sample.pricing_commodity}"

    insurer_companies = get_insurers_for_trade(trade)

    for i, insurer_company in enumerate(insurer_companies):
        expected_name = insurer_company.name if insurer_company else "unknown"
        expected_iso2 = (
            insurer_company.country_iso2
            if insurer_company and insurer_company.name != "unknown"
            else "unknown"
        )
        actual_name = sample.ship_insurer_names[i]
        actual_iso2 = sample.ship_insurer_iso2s[i]
        assert (
            actual_name == expected_name
        ), f"Computed insurer is wrong for {sample_id}: expected {expected_name} got {actual_name}"
        assert (
            actual_iso2 == expected_iso2
        ), f"Computed insurer country is wrong for {sample_id}: expected {expected_iso2} got {actual_iso2}"

    owner_companies = get_owners_for_trade(trade)

    for i, owner_company in enumerate(owner_companies):
        expected_name = owner_company.name if owner_company else "unknown"
        expected_iso2 = (
            owner_company.country_iso2
            if owner_company
            and owner_company.name != "unknown"
            and owner_company.country_iso2 is not None
            else "unknown"
        )
        actual_name = sample.ship_owner_names[i]
        actual_iso2 = sample.ship_owner_iso2s[i]
        assert (
            actual_name == expected_name
        ), f"Computed owner is wrong for {sample_id}: expected {expected_name} got {actual_name}"
        assert (
            actual_iso2 == expected_iso2
        ), f"Computed owner country is wrong for {sample_id}: expected {expected_iso2} got {actual_iso2}"

    destination_zone = (
        session.query(KplerZone).filter(KplerZone.id == trade.arrival_zone_id).first()
    )

    flag_for_trade = get_flags_for_trade(trade)

    for i, expected_iso2 in enumerate(flag_for_trade):
        actual_iso2 = sample.ship_flag_iso2s[i]
        assert (
            actual_iso2 == expected_iso2
        ), f"Computed flag country is wrong for {sample_id}: expected {expected_iso2} got {actual_iso2}"

    expected_price = get_expected_price(
        sample=sample,
        trade=trade,
        origin_zone=departure_zone,
        destination_zone=destination_zone,
        commodity_id=expected_commodity,
        insurer_details=insurer_companies,
        owner_details=owner_companies,
    )

    assert sample.eur_per_tonne == (
        expected_price.eur_per_tonne if expected_price else None
    ), f"Computed price is wrong for {sample_id}: expected {expected_price.eur_per_tonne} got {sample.eur_per_tonne}"

    kpler_vessels_for_trade = (
        session.query(
            KplerVessel.imo,
            KplerVessel.type_class_name,
            KplerVessel.type_name,
            KplerVessel.capacity_cm,
        )
        .filter(KplerVessel.imo.in_(trade.vessel_imos))
        .all()
    )

    largest_vessel = max(kpler_vessels_for_trade, key=lambda x: x.capacity_cm, default=None)

    expected_name = "unknown"
    if largest_vessel:
        if largest_vessel.type_class_name:
            expected_name = largest_vessel.type_class_name
        elif largest_vessel.type_name:
            expected_name = largest_vessel.type_name

    assert (
        sample.largest_vessel_type == expected_name
    ), f"Computed largest vessel type is wrong for {sample_id}: expected {largest_vessel.type_class_name} got {sample.largest_vessel_type}"
    assert (
        sample.largest_vessel_capacity_cm == largest_vessel.capacity_cm
    ), f"Computed largest vessel capacity is wrong for {sample_id}: expected {largest_vessel.capacity_cm} got {sample.largest_vessel_capacity_cm}"


def get_expected_price(
    *,
    sample: KplerTradeComputed,
    trade: KplerTrade,
    origin_zone: KplerZone,
    destination_zone: KplerZone,
    commodity_id: str,
    insurer_details: list[Company],
    owner_details: list[Company],
):
    prices: list[Price] = (
        session.query(Price)
        .filter(
            Price.scenario == sample.pricing_scenario,
            Price.commodity == commodity_id,
            Price.date == trade.departure_date_utc.date(),
        )
        .all()
    )

    if origin_zone.country_iso2 != "RU":

        return [
            price
            for price in prices
            if price.destination_iso2s[0] is None
            and price.ship_insurer_iso2s[0] is None
            and price.ship_owner_iso2s[0] is None
        ][0]

    else:
        prices = [
            get_price_for_trade_ship(prices, destination_zone, insurer_details[i], owner_details[i])
            for (i, _) in enumerate(trade.vessel_imos)
        ]

        not_null_prices = [price for price in prices if price.eur_per_tonne is not None]

        min_price = min(not_null_prices, key=lambda x: x.eur_per_tonne, default=None)

        # Get minimum price
        return min_price


def get_price_for_trade_ship(
    prices: list[Price],
    destination_zone: KplerZone,
    insurer_details: Company,
    owner_details: Company,
):
    def rank_price(price: Price):
        # Ranks the price based on matching the destination, insurer and owner.
        # Higher number if it matches more.
        return (
            (
                100
                if destination_zone and (destination_zone.country_iso2 in price.destination_iso2s)
                else 0
            )
            + (
                10
                if insurer_details and (insurer_details.country_iso2 in price.ship_insurer_iso2s)
                else 0
            )
            + (1 if owner_details and (owner_details.country_iso2 in price.ship_owner_iso2s) else 0)
        )

    matched_prices = [
        price
        for price in prices
        if (
            (destination_zone and (destination_zone.country_iso2 in price.destination_iso2s))
            or price.destination_iso2s[0] is None
        )
        and (
            (insurer_details and (insurer_details.country_iso2 in price.ship_insurer_iso2s))
            or price.ship_insurer_iso2s[0] is None
        )
        and (
            (owner_details and (owner_details.country_iso2 in price.ship_owner_iso2s))
            or price.ship_owner_iso2s[0] is None
        )
    ]

    ranked_prices = sorted(matched_prices, key=rank_price, reverse=True)

    return ranked_prices[0]


def extract_flag_iso2(flag: ShipFlag):
    if not flag:
        return "unknown"
    if not flag.flag_iso2:
        return "unknown"
    return flag.flag_iso2


def get_flags_for_trade(trade: KplerTrade):
    return [
        extract_flag_iso2(get_flag_for_trade_ship(trade, ship_imo))
        for ship_imo in trade.vessel_imos
    ]


def get_flag_for_trade_ship(trade: KplerTrade, ship_imo: str):
    flag: ShipFlag = (
        session.query(ShipFlag)
        .filter(
            ShipFlag.imo == ship_imo,
            sa.or_(
                ShipFlag.first_seen < trade.departure_date_utc,
                ShipFlag.first_seen == None,
            ),
        )
        .order_by(nulls_last(ShipFlag.first_seen.desc()))
        .first()
    )

    return flag


def get_insurers_for_trade(trade: KplerTrade):
    return [get_insurer_for_trade_ship(trade, ship_imo) for ship_imo in trade.vessel_imos]


def get_insurer_for_trade_ship(trade: KplerTrade, ship_imo: str):

    insurer: ShipInsurer = (
        session.query(ShipInsurer)
        .filter(
            ShipInsurer.ship_imo == ship_imo,
            ShipInsurer.is_valid,
            sa.or_(
                func.coalesce(ShipInsurer.date_from_insurer, ShipInsurer.date_from_equasis)
                <= trade.departure_date_utc + dt.timedelta(days=14),
                func.coalesce(ShipInsurer.date_from_insurer, ShipInsurer.date_from_equasis) == None,
            ),
        )
        .order_by(
            nulls_last(
                func.coalesce(ShipInsurer.date_from_insurer, ShipInsurer.date_from_equasis).desc()
            ),
            nulls_last(ShipInsurer.updated_on.desc()),
        )
        .first()
    )

    if not insurer:
        return None

    insurer_details: Company = (
        session.query(Company).filter(Company.id == insurer.company_id).first()
    )

    return insurer_details


def get_owners_for_trade(trade: KplerTrade):
    return [get_owner_for_trade_ship(trade, ship_imo) for ship_imo in trade.vessel_imos]


def get_owner_for_trade_ship(trade: KplerTrade, ship_imo: str):
    owner: ShipOwner = (
        session.query(ShipOwner)
        .filter(
            ShipOwner.ship_imo == ship_imo,
            sa.or_(
                ShipOwner.date_from <= trade.departure_date_utc + dt.timedelta(days=14),
                ShipOwner.date_from == None,
            ),
        )
        .order_by(nulls_last(ShipOwner.date_from.desc()), nulls_last(ShipOwner.updated_on.desc()))
        .first()
    )

    if not owner:
        return None

    owner_details: Company = session.query(Company).filter(Company.id == owner.company_id).first()

    return owner_details


def extract_expected_pricing_commodity(product, commodity, departure_zone):
    expected_commodity = commodity.pricing_commodity

    if (
        product.group_name == "Crude/Co"
        and product.grade_name not in ["CPC Kazakhstan", "KEBCO"]
        and product.grade_name != None
        and departure_zone.country_iso2 == "RU"
        and departure_zone.port_name
        and re.match("^Nakhodka|^De Kast|^Prigorod", departure_zone.port_name)
    ):
        expected_commodity = "crude_oil_espo"
    elif (
        product.group_name == "Crude/Co"
        and departure_zone.country_iso2 == "RU"
        and product.grade_name not in ["CPC Kazakhstan", "KEBCO"]
        and product.grade_name != None
    ):
        expected_commodity = "crude_oil_urals"
    return expected_commodity


def extract_expected_commodity_id(product):
    product_name: str = product.commodity_name if product.commodity_name else product.group_name
    expected_name = "kpler_" + product_name.replace(" ", "_").replace("/", "_").lower()
    return expected_name
