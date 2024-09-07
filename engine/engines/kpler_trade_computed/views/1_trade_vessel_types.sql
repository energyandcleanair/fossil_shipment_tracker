CREATE MATERIALIZED VIEW ktc_trade_vessel_types_temp AS
SELECT
  ktc_trade_ship_temp.trade_id AS trade_id,
  ktc_trade_ship_temp.flow_id AS flow_id,
  array_agg(
    COALESCE(
      kpler_vessel.type_class_name,
      kpler_vessel.type_name,
      'unknown'
    )
    order by
      ktc_trade_ship_temp.ship_order
  ) AS vessel_types,
  array_agg(
    kpler_vessel.capacity_cm
    order by
      ktc_trade_ship_temp.ship_order
  ) as vessel_capacities_cm
FROM
  ktc_trade_ship_temp
  LEFT JOIN kpler_vessel ON ktc_trade_ship_temp.ship_imo = kpler_vessel.imo
GROUP BY
  ktc_trade_ship_temp.trade_id,
  ktc_trade_ship_temp.flow_id;

CREATE INDEX ON ktc_trade_vessel_types_temp (trade_id);

CREATE INDEX ON ktc_trade_vessel_types_temp (flow_id);

ANALYZE ktc_trade_vessel_types_temp;
