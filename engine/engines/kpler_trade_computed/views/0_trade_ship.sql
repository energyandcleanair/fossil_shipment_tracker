CREATE MATERIALIZED VIEW ktc_trade_ship_temp AS
SELECT
  kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  ships.ship_order AS ship_order,
  ships.ship_imo AS ship_imo
FROM
  kpler_trade,
  unnest(kpler_trade.vessel_imos) WITH ORDINALITY AS ships(ship_imo, ship_order)
WHERE
  kpler_trade.is_valid
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  ships.ship_order;

CREATE INDEX ON ktc_trade_ship_temp (trade_id);

CREATE INDEX ON ktc_trade_ship_temp (flow_id);

CREATE INDEX ON ktc_trade_ship_temp (ship_imo);

ANALYZE ktc_trade_ship_temp;
