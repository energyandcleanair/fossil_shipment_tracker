CREATE MATERIALIZED VIEW ktc_voyage_flag AS
SELECT
  DISTINCT ON (
    kpler_trade.id,
    kpler_trade.flow_id,
    ktc_trade_ship.ship_order,
    ktc_trade_ship.ship_imo
  ) kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  ktc_trade_ship.ship_order AS ship_order,
  ktc_trade_ship.ship_imo AS ship_imo,
  coalesce(ship_flag.flag_iso2, 'unknown') AS iso2,
  'PCC' = any(country.regions) AS in_pcc,
  country.iso2 = 'NO' AS in_norway
FROM
  kpler_trade
  JOIN ktc_trade_ship ON kpler_trade.id = ktc_trade_ship.trade_id
  AND kpler_trade.flow_id = ktc_trade_ship.flow_id
  LEFT OUTER JOIN ship_flag ON ship_flag.imo = ktc_trade_ship.ship_imo
  LEFT OUTER JOIN country ON country.iso2 = ship_flag.flag_iso2
WHERE
  kpler_trade.is_valid
  AND (
    ship_flag.first_seen IS NULL
    OR ship_flag.first_seen < kpler_trade.departure_date_utc
  )
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  ktc_trade_ship.ship_order,
  ktc_trade_ship.ship_imo,
  ship_flag.first_seen DESC NULLS LAST;

CREATE INDEX ON ktc_voyage_flag (trade_id);

CREATE INDEX ON ktc_voyage_flag (flow_id);

CREATE INDEX ON ktc_voyage_flag (ship_imo);

ANALYZE ktc_voyage_flag;
