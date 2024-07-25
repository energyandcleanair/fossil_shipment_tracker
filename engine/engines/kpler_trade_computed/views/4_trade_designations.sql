CREATE MATERIALIZED VIEW ktc_trade_designations AS
SELECT
  kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  array_agg(
    coalesce(ktc_voyage_owner.name, 'unknown')
    ORDER BY
      ktc_trade_ship.ship_order
  ) AS ship_owner_names,
  array_agg(
    ktc_voyage_owner.iso2
    ORDER BY
      ktc_trade_ship.ship_order
  ) AS ship_owner_iso2s,
  array_agg(
    ktc_voyage_owner.region
    ORDER BY
      ktc_trade_ship.ship_order
  ) AS ship_owner_regions,
  bool_or(ktc_voyage_owner.in_pcc) AS owned_in_pcc,
  bool_or(ktc_voyage_owner.in_norway) AS owned_in_norway,
  bool_or(ktc_voyage_owner.iso2 IS NOT NULL) AS owner_known,
  array_agg(
    coalesce(ktc_voyage_insurer.name, 'unknown')
    ORDER BY
      ktc_trade_ship.ship_order
  ) AS ship_insurer_names,
  array_agg(
    ktc_voyage_insurer.iso2
    ORDER BY
      ktc_trade_ship.ship_order
  ) AS ship_insurer_iso2s,
  array_agg(
    ktc_voyage_insurer.region
    ORDER BY
      ktc_trade_ship.ship_order
  ) AS ship_insurer_regions,
  bool_or(ktc_voyage_insurer.in_pcc) AS insured_in_pcc,
  bool_or(ktc_voyage_insurer.in_norway) AS insured_in_norway,
  array_agg(
    coalesce(ktc_voyage_flag.iso2, 'unknown')
    ORDER BY
      ktc_trade_ship.ship_order
  ) AS ship_flag_iso2s,
  bool_or(ktc_voyage_flag.in_pcc) AS flag_in_pcc,
  bool_or(ktc_voyage_flag.in_norway) AS flag_in_norway,
  array_agg(
    ktc_crea_designation.crea_designation
    ORDER BY
      ktc_trade_ship.ship_order
  ) AS crea_designations
FROM
  kpler_trade
  JOIN ktc_trade_ship ON kpler_trade.id = ktc_trade_ship.trade_id
  AND kpler_trade.flow_id = ktc_trade_ship.flow_id
  LEFT OUTER JOIN ktc_voyage_owner ON ktc_voyage_owner.trade_id = kpler_trade.id
  AND ktc_voyage_owner.flow_id = kpler_trade.flow_id
  AND ktc_voyage_owner.ship_imo = ktc_trade_ship.ship_imo
  LEFT OUTER JOIN ktc_voyage_insurer ON ktc_voyage_insurer.trade_id = kpler_trade.id
  AND ktc_voyage_insurer.flow_id = kpler_trade.flow_id
  AND ktc_voyage_insurer.ship_imo = ktc_trade_ship.ship_imo
  LEFT OUTER JOIN ktc_voyage_flag ON ktc_voyage_flag.trade_id = kpler_trade.id
  AND ktc_voyage_flag.flow_id = kpler_trade.flow_id
  AND ktc_voyage_flag.ship_imo = ktc_trade_ship.ship_imo
  LEFT OUTER JOIN ktc_crea_designation ON ktc_crea_designation.trade_id = kpler_trade.id
  AND ktc_crea_designation.flow_id = kpler_trade.flow_id
  AND ktc_crea_designation.ship_imo = ktc_trade_ship.ship_imo
WHERE
  kpler_trade.is_valid
GROUP BY
  kpler_trade.id,
  kpler_trade.flow_id
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id;

CREATE INDEX ON ktc_trade_designations (trade_id);

CREATE INDEX ON ktc_trade_designations (flow_id);

ANALYZE ktc_trade_designations;
