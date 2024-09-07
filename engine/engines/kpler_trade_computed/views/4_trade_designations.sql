CREATE MATERIALIZED VIEW ktc_trade_designations_temp AS
SELECT
  kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  array_agg(
    coalesce(ktc_voyage_owner_temp.name, 'unknown')
    ORDER BY
      ktc_trade_ship_temp.ship_order
  ) AS ship_owner_names,
  array_agg(
    ktc_voyage_owner_temp.iso2
    ORDER BY
      ktc_trade_ship_temp.ship_order
  ) AS ship_owner_iso2s,
  array_agg(
    ktc_voyage_owner_temp.region
    ORDER BY
      ktc_trade_ship_temp.ship_order
  ) AS ship_owner_regions,
  bool_or(ktc_voyage_owner_temp.in_pcc) AS owned_in_pcc,
  bool_or(ktc_voyage_owner_temp.in_norway) AS owned_in_norway,
  bool_or(ktc_voyage_owner_temp.iso2 IS NOT NULL) AS owner_known,
  array_agg(
    coalesce(ktc_voyage_insurer_temp.name, 'unknown')
    ORDER BY
      ktc_trade_ship_temp.ship_order
  ) AS ship_insurer_names,
  array_agg(
    ktc_voyage_insurer_temp.iso2
    ORDER BY
      ktc_trade_ship_temp.ship_order
  ) AS ship_insurer_iso2s,
  array_agg(
    ktc_voyage_insurer_temp.region
    ORDER BY
      ktc_trade_ship_temp.ship_order
  ) AS ship_insurer_regions,
  bool_or(ktc_voyage_insurer_temp.in_pcc) AS insured_in_pcc,
  bool_or(ktc_voyage_insurer_temp.in_norway) AS insured_in_norway,
  array_agg(
    coalesce(ktc_voyage_flag_temp.iso2, 'unknown')
    ORDER BY
      ktc_trade_ship_temp.ship_order
  ) AS ship_flag_iso2s,
  bool_or(ktc_voyage_flag_temp.in_pcc) AS flag_in_pcc,
  bool_or(ktc_voyage_flag_temp.in_norway) AS flag_in_norway,
  array_agg(
    ktc_crea_designation_temp.crea_designation
    ORDER BY
      ktc_trade_ship_temp.ship_order
  ) AS crea_designations
FROM
  kpler_trade
  JOIN ktc_trade_ship_temp ON kpler_trade.id = ktc_trade_ship_temp.trade_id
  AND kpler_trade.flow_id = ktc_trade_ship_temp.flow_id
  LEFT OUTER JOIN ktc_voyage_owner_temp ON ktc_voyage_owner_temp.trade_id = kpler_trade.id
  AND ktc_voyage_owner_temp.flow_id = kpler_trade.flow_id
  AND ktc_voyage_owner_temp.ship_order = ktc_trade_ship_temp.ship_order
  AND ktc_voyage_owner_temp.ship_imo = ktc_trade_ship_temp.ship_imo
  LEFT OUTER JOIN ktc_voyage_insurer_temp ON ktc_voyage_insurer_temp.trade_id = kpler_trade.id
  AND ktc_voyage_insurer_temp.flow_id = kpler_trade.flow_id
  AND ktc_voyage_insurer_temp.ship_order = ktc_trade_ship_temp.ship_order
  AND ktc_voyage_insurer_temp.ship_imo = ktc_trade_ship_temp.ship_imo
  LEFT OUTER JOIN ktc_voyage_flag_temp ON ktc_voyage_flag_temp.trade_id = kpler_trade.id
  AND ktc_voyage_flag_temp.flow_id = kpler_trade.flow_id
  AND ktc_voyage_flag_temp.ship_order = ktc_trade_ship_temp.ship_order
  AND ktc_voyage_flag_temp.ship_imo = ktc_trade_ship_temp.ship_imo
  LEFT OUTER JOIN ktc_crea_designation_temp ON ktc_crea_designation_temp.trade_id = kpler_trade.id
  AND ktc_crea_designation_temp.flow_id = kpler_trade.flow_id
  AND ktc_crea_designation_temp.ship_order = ktc_trade_ship_temp.ship_order
  AND ktc_crea_designation_temp.ship_imo = ktc_trade_ship_temp.ship_imo
WHERE
  kpler_trade.is_valid
GROUP BY
  kpler_trade.id,
  kpler_trade.flow_id
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id;

CREATE INDEX ON ktc_trade_designations_temp (trade_id);

CREATE INDEX ON ktc_trade_designations_temp (flow_id);

ANALYZE ktc_trade_designations_temp;
