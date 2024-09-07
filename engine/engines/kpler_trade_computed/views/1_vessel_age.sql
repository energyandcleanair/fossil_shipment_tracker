CREATE MATERIALIZED VIEW ktc_vessel_age_temp AS
SELECT
  kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  ktc_trade_ship_temp.ship_order AS ship_order,
  ktc_trade_ship_temp.ship_imo AS ship_imo,
  EXTRACT(
    epoch
    FROM
      age(
        kpler_trade.departure_date_utc,
        kpler_vessel.build_date
      )
  ) / 31536000 AS vessel_age
FROM
  kpler_trade
  JOIN ktc_trade_ship_temp ON kpler_trade.id = ktc_trade_ship_temp.trade_id
  AND kpler_trade.flow_id = ktc_trade_ship_temp.flow_id
  LEFT OUTER JOIN kpler_vessel ON ktc_trade_ship_temp.ship_imo = kpler_vessel.imo
WHERE
  kpler_trade.is_valid
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  ktc_trade_ship_temp.ship_order;

CREATE INDEX ON ktc_vessel_age_temp (trade_id);

CREATE INDEX ON ktc_vessel_age_temp (flow_id);

ANALYZE ktc_vessel_age_temp;
