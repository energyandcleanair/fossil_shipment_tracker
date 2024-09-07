CREATE MATERIALIZED VIEW ktc_trade_price_temp AS
SELECT
  DISTINCT ON (
    kpler_trade.id,
    kpler_trade.flow_id,
    price.scenario
  ) kpler_trade.id as trade_id,
  kpler_trade.flow_id,
  price.scenario AS scenario,
  price.id AS price_id
FROM
  kpler_trade
  JOIN ktc_trade_ship_price_temp ON kpler_trade.id = ktc_trade_ship_price_temp.trade_id
  AND kpler_trade.flow_id = ktc_trade_ship_price_temp.flow_id
  JOIN price ON ktc_trade_ship_price_temp.price_id = price.id
WHERE
  kpler_trade.is_valid
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  price.scenario,
  price.eur_per_tonne NULLS LAST;

CREATE INDEX ON ktc_trade_price_temp (trade_id);

CREATE INDEX ON ktc_trade_price_temp (flow_id);

CREATE INDEX ON ktc_trade_price_temp (price_id);

ANALYZE ktc_trade_price_temp;
