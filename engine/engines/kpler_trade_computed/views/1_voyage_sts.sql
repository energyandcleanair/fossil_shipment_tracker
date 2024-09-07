CREATE MATERIALIZED VIEW ktc_voyage_sts_temp AS WITH voyage_sts_zones AS (
  SELECT
    kpler_trade.id AS trade_id,
    kpler_trade.flow_id AS flow_id,
    ktc_trade_ship_temp.ship_order AS ship_order,
    ktc_trade_ship_temp.ship_imo AS ship_imo,
    CASE
      WHEN (
        ktc_trade_ship_temp.ship_order = 1
        AND kpler_trade.departure_sts
      ) THEN departure_zone_id
      WHEN ktc_trade_ship_temp.ship_order > 1 THEN kpler_trade.step_zone_ids [ktc_trade_ship_temp.ship_order - 1]
      ELSE NULL
    END AS start_sts_zone_id,
    CASE
      WHEN (
        ktc_trade_ship_temp.ship_order = array_length(kpler_trade.vessel_imos, 1)
        AND kpler_trade.arrival_sts
      ) THEN arrival_zone_id
      WHEN ktc_trade_ship_temp.ship_order < array_length(kpler_trade.vessel_imos, 1) THEN kpler_trade.step_zone_ids [ktc_trade_ship_temp.ship_order]
      ELSE NULL
    END AS end_sts_zone_id
  FROM
    kpler_trade
    JOIN ktc_trade_ship_temp ON (
      kpler_trade.id = ktc_trade_ship_temp.trade_id
      AND kpler_trade.flow_id = ktc_trade_ship_temp.flow_id
    )
  WHERE
    kpler_trade.is_valid
)
SELECT
  voyage_sts_zones.trade_id AS trade_id,
  voyage_sts_zones.flow_id AS flow_id,
  voyage_sts_zones.ship_order AS ship_order,
  voyage_sts_zones.ship_imo AS ship_imo,
  start_sts_zone_id AS start_sts_zone_id,
  start_sts_zone.name AS start_sts_zone_name,
  start_sts_zone.country_iso2 AS start_sts_iso2,
  start_sts_country.region AS start_sts_region,
  end_sts_zone_id AS end_sts_zone_id,
  end_sts_zone.name AS end_sts_zone_name,
  end_sts_zone.country_iso2 AS end_sts_iso2,
  end_sts_country.region AS end_sts_region
FROM
  voyage_sts_zones
  LEFT OUTER JOIN kpler_zone AS start_sts_zone ON voyage_sts_zones.start_sts_zone_id = start_sts_zone.id
  LEFT OUTER JOIN country AS start_sts_country ON start_sts_zone.country_iso2 = start_sts_country.iso2
  LEFT OUTER JOIN kpler_zone AS end_sts_zone ON voyage_sts_zones.end_sts_zone_id = end_sts_zone.id
  LEFT OUTER JOIN country AS end_sts_country ON end_sts_zone.country_iso2 = end_sts_country.iso2;

CREATE INDEX ON ktc_voyage_sts_temp (trade_id);

CREATE INDEX ON ktc_voyage_sts_temp (flow_id);

CREATE INDEX ON ktc_voyage_sts_temp (ship_order);

CREATE INDEX ON ktc_voyage_sts_temp (ship_imo);

ANALYZE ktc_voyage_sts_temp;
