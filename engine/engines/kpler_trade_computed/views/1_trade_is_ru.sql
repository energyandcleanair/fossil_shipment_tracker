CREATE MATERIALIZED VIEW ktc_trade_is_ru AS
SELECT
    ktc_trade_ship.trade_id,
    ktc_trade_ship.flow_id,
    ktc_trade_ship.ship_order,
    ktc_trade_ship.ship_imo,
    kpler_trade.departure_date_utc,
    CASE
        WHEN kpler_zone.country_iso2 = 'RU'
        AND kpler_product.grade_name NOT IN ('CPC Kazakhstan', 'KEBCO') THEN 1
        ELSE 0
    END AS ru_trade
FROM
    ktc_trade_ship
    LEFT JOIN kpler_trade ON ktc_trade_ship.trade_id = kpler_trade.id
    AND ktc_trade_ship.flow_id = kpler_trade.flow_id
    LEFT JOIN kpler_zone ON kpler_trade.departure_zone_id = kpler_zone.id
    LEFT JOIN kpler_product ON kpler_trade.product_id = kpler_product.id
ORDER BY
    ktc_trade_ship.ship_imo,
    kpler_trade.departure_date_utc;

CREATE INDEX ON ktc_trade_is_ru (trade_id);

CREATE INDEX ON ktc_trade_is_ru (flow_id);

CREATE INDEX ON ktc_trade_is_ru (ship_imo);

ANALYZE ktc_trade_is_ru;
