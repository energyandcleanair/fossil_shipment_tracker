CREATE MATERIALIZED VIEW ktc_trade_vessel_type AS
SELECT
  DISTINCT ON (
    ktc_trade_ship.trade_id,
    ktc_trade_ship.flow_id
  ) ktc_trade_ship.trade_id AS trade_id,
  ktc_trade_ship.flow_id AS flow_id,
  COALESCE(
    kpler_vessel.type_class_name,
    kpler_vessel.type_name,
    'unknown'
  ) AS largest_vessel_type,
  kpler_vessel.capacity_cm AS largest_vessel_capacity_cm
FROM
  ktc_trade_ship
  LEFT JOIN kpler_vessel ON ktc_trade_ship.ship_imo = kpler_vessel.imo
ORDER BY
  ktc_trade_ship.trade_id,
  ktc_trade_ship.flow_id,
  kpler_vessel.capacity_cm DESC NULLS LAST;

CREATE INDEX ON ktc_trade_vessel_type (trade_id);

CREATE INDEX ON ktc_trade_vessel_type (flow_id);

ANALYZE ktc_trade_vessel_type;
