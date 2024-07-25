CREATE MATERIALIZED VIEW ktc_recent_ru_trade_percent AS
SELECT
    ktc_trade_is_ru.trade_id,
    ktc_trade_is_ru.flow_id,
    ktc_trade_is_ru.ship_order,
    ktc_trade_is_ru.ship_imo,
    AVG(ru_trade) OVER (
        PARTITION BY ktc_trade_is_ru.ship_imo
        ORDER BY
            ktc_trade_is_ru.departure_date_utc ROWS BETWEEN 4 PRECEDING
            AND CURRENT ROW
    ) recent_ru_percent
FROM
    ktc_trade_is_ru
ORDER BY
    ktc_trade_is_ru.trade_id,
    ktc_trade_is_ru.flow_id,
    ktc_trade_is_ru.ship_order;

CREATE INDEX ON ktc_recent_ru_trade_percent (trade_id);

CREATE INDEX ON ktc_recent_ru_trade_percent (flow_id);

CREATE INDEX ON ktc_recent_ru_trade_percent (ship_imo);

ANALYZE ktc_recent_ru_trade_percent;
