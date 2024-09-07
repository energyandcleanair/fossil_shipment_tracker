CREATE MATERIALIZED VIEW ktc_recent_ru_trade_percent_temp AS
SELECT
    ktc_trade_is_ru_temp.trade_id,
    ktc_trade_is_ru_temp.flow_id,
    ktc_trade_is_ru_temp.ship_order,
    ktc_trade_is_ru_temp.ship_imo,
    AVG(ru_trade) OVER (
        PARTITION BY ktc_trade_is_ru_temp.ship_imo
        ORDER BY
            ktc_trade_is_ru_temp.departure_date_utc ROWS BETWEEN 4 PRECEDING
            AND CURRENT ROW
    ) recent_ru_percent
FROM
    ktc_trade_is_ru_temp
ORDER BY
    ktc_trade_is_ru_temp.trade_id,
    ktc_trade_is_ru_temp.flow_id,
    ktc_trade_is_ru_temp.ship_order;

CREATE INDEX ON ktc_recent_ru_trade_percent_temp (trade_id);

CREATE INDEX ON ktc_recent_ru_trade_percent_temp (flow_id);

CREATE INDEX ON ktc_recent_ru_trade_percent_temp (ship_imo);

ANALYZE ktc_recent_ru_trade_percent_temp;
