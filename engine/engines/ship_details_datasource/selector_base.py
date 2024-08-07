from sqlalchemy import func, case
from sqlalchemy.sql.expression import Subquery
import sqlalchemy as sa
from base.db import session
from base.models import Commodity, KplerProduct, KplerTrade, KplerZone, KplerTradeComputedShips

import base

COMMODITY_SETTINGS = {
    base.CRUDE_OIL: {"known": 30, "unknown": 3, "commodity_update_priority": 0},
    base.OIL_PRODUCTS: {"known": 30, "unknown": 3, "commodity_update_priority": 0},
    base.OIL_OR_CHEMICAL: {"known": 30, "unknown": 3, "commodity_update_priority": 0},
    base.LNG: {"known": 30, "unknown": 3, "commodity_update_priority": 1},
    base.COAL: {"known": 30, "unknown": 15, "commodity_update_priority": 2},
    base.BULK: {"known": 30, "unknown": 15, "commodity_update_priority": 2},
    base.LPG: {"known": 30, "unknown": 3, "commodity_update_priority": 3},
}


def build_filter_query() -> Subquery:
    """
    A query to list all ships and their departure dates, countries, commodities, and update
    priority based on their voyage history.
    """

    commodity_id_field = (
        "kpler_"
        + sa.func.replace(
            sa.func.replace(
                sa.func.lower(func.coalesce(KplerProduct.commodity_name, KplerProduct.group_name)),
                " ",
                "_",
            ),
            "/",
            "_",
        )
    ).label("commodity")

    priority_field = case(
        (
            sa.and_(KplerZone.country_iso2 == "RU", KplerTrade.departure_date_utc > "2022-01-01"),
            2,
        ),
        (KplerZone.country_iso2 == "RU", 1),
        else_=0,
    ).label("priority")

    kpler_ships = (
        session.query(
            KplerTradeComputedShips.vessel_imo.label("ship_imo"),
            KplerTrade.departure_date_utc.label("date_utc"),
            KplerZone.country_iso2.label("country_iso2"),
            Commodity.equivalent_id.label("commodity"),
            sa.sql.expression.literal("kpler").label("source"),
            priority_field,
        )
        .outerjoin(
            KplerTrade,
            sa.and_(
                KplerTradeComputedShips.trade_id == KplerTrade.id,
                KplerTradeComputedShips.flow_id == KplerTrade.flow_id,
            ),
        )
        .outerjoin(KplerProduct, KplerTrade.product_id == KplerProduct.id)
        .outerjoin(KplerZone, KplerTrade.departure_zone_id == KplerZone.id)
        .outerjoin(Commodity, commodity_id_field == Commodity.id)
        .filter(KplerTrade.departure_date_utc.isnot(None))
        .order_by(
            KplerTradeComputedShips.vessel_imo,
            Commodity.equivalent_id,
            priority_field.desc(),
        )
        .distinct(KplerTradeComputedShips.vessel_imo, Commodity.equivalent_id)
    )

    return kpler_ships.subquery()
