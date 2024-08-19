CREATE MATERIALIZED VIEW ktc_trade_vessel_types AS
SELECT
  ktc_trade_ship.trade_id AS trade_id,
  ktc_trade_ship.flow_id AS flow_id,
  array_agg(
  	COALESCE(
	    kpler_vessel.type_class_name,
	    kpler_vessel.type_name,
	    'unknown'
	  )
	  order by ktc_trade_ship.ship_order
	) AS vessel_types,
  array_agg(
  	kpler_vessel.capacity_cm
  	order by ktc_trade_ship.ship_order
  ) as vessel_capacities_cm
FROM
  ktc_trade_ship
  LEFT JOIN kpler_vessel ON ktc_trade_ship.ship_imo = kpler_vessel.imo
GROUP BY
  ktc_trade_ship.trade_id,
  ktc_trade_ship.flow_id;

CREATE INDEX ON ktc_trade_vessel_types (trade_id);

CREATE INDEX ON ktc_trade_vessel_types (flow_id);

ANALYZE ktc_trade_vessel_types;
