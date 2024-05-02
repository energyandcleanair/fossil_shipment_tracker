CREATE MATERIALIZED VIEW ktc_trade_step_zones AS
SELECT
  kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  array_agg(
    kpler_zone.name
    ORDER BY
      step_zone.step_zone_order
  ) AS step_zone_names,
  array_agg(
    kpler_zone.country_iso2
    ORDER BY
      step_zone.step_zone_order
  ) AS step_zone_iso2s,
  array_agg(
    country.region
    ORDER BY
      step_zone.step_zone_order
  ) AS step_zone_regions
FROM
  kpler_trade,
  unnest(kpler_trade.step_zone_ids) WITH ORDINALITY AS step_zone(step_zone_id, step_zone_order)
  LEFT OUTER JOIN kpler_zone ON step_zone.step_zone_id = kpler_zone.id
  LEFT OUTER JOIN country ON kpler_zone.country_iso2 = country.iso2
WHERE
  kpler_trade.is_valid
GROUP BY
  kpler_trade.id,
  kpler_trade.flow_id
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id;

CREATE INDEX ON ktc_trade_step_zones (trade_id);

CREATE INDEX ON ktc_trade_step_zones (flow_id);

ANALYZE ktc_trade_step_zones;
