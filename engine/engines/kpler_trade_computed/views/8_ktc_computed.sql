CREATE MATERIALIZED VIEW ktc_kpler_trade_computed_temp AS
SELECT
  kpler_trade.id AS trade_id,
  kpler_trade.flow_id,
  kpler_trade.product_id,
  kpler_trade.vessel_imos,
  commodity.id AS kpler_product_commodity_id,
  price.scenario AS pricing_scenario,
  price.commodity AS pricing_commodity,
  price.eur_per_tonne,
  ktc_trade_designations_temp.ship_insurer_names,
  ktc_trade_designations_temp.ship_insurer_iso2s,
  ktc_trade_designations_temp.ship_insurer_regions,
  ktc_trade_designations_temp.ship_owner_names,
  ktc_trade_designations_temp.ship_owner_iso2s,
  ktc_trade_designations_temp.ship_owner_regions,
  CASE
    WHEN (
      ktc_trade_designations_temp.insured_in_pcc
      OR ktc_trade_designations_temp.owned_in_pcc
    ) THEN 'Owned and / or insured in EU & G7'
    WHEN ktc_trade_designations_temp.insured_in_norway THEN 'Insured in Norway'
    WHEN ktc_trade_designations_temp.owner_known THEN 'Others'
    ELSE 'Unknown'
  END AS ownership_sanction_coverage,
  ktc_trade_designations_temp.ship_flag_iso2s,
  CASE
    WHEN (
      ktc_trade_designations_temp.flag_in_pcc
      OR ktc_trade_designations_temp.flag_in_norway
    ) THEN 'Flag in PCC'
    WHEN ktc_trade_designations_temp.flag_in_norway THEN 'Flag in Norway'
    ELSE 'Others'
  END AS flag_sanction_coverage,
  ktc_trade_designations_temp.flag_in_pcc,
  coalesce(
    ktc_trade_step_zones_temp.step_zone_names,
    ARRAY [] :: varchar []
  ) AS step_zone_names,
  coalesce(
    ktc_trade_step_zones_temp.step_zone_iso2s,
    ARRAY [] :: varchar []
  ) AS step_zone_iso2s,
  coalesce(
    ktc_trade_step_zones_temp.step_zone_regions,
    ARRAY [] :: varchar []
  ) AS step_zone_regions,
  coalesce(
    kpler_trade.step_zone_ids,
    ARRAY [] :: numeric []
  ) AS step_zone_ids,
  ktc_vessel_ages_for_trade_temp.vessel_ages,
  ktc_vessel_ages_for_trade_temp.avg_vessel_age,
  ktc_trade_largest_vessel_temp.largest_vessel_type,
  ktc_trade_largest_vessel_temp.largest_vessel_capacity_cm,
  coalesce(
    ktc_trade_vessel_types_temp.vessel_types,
    ARRAY [] :: varchar []
  ) AS vessel_types,
  coalesce(
    ktc_trade_vessel_types_temp.vessel_capacities_cm,
    ARRAY [] :: numeric []
  ) AS vessel_capacities_cm,
  coalesce(
    ktc_trade_designations_temp.crea_designations,
    ARRAY [] :: varchar []
  ) AS crea_designations,
  coalesce(
    ktc_trade_inspections_temp.n_inspections_2y,
    ARRAY [] :: numeric []
  ) AS n_inspections_2y,
  coalesce(
    ktc_trade_inspections_temp.deficiencies_per_inspection_2y,
    ARRAY [] :: numeric []
  ) AS deficiencies_per_inspection_2y,
  coalesce(
    ktc_trade_inspections_temp.detentions_per_inspection_2y,
    ARRAY [] :: numeric []
  ) AS detentions_per_inspection_2y,
  coalesce(
    ktc_trade_inspections_temp.n_detentions_2y,
    ARRAY [] :: numeric []
  ) AS n_detentions_2y,
  ktc_trade_inspections_temp.avg_n_inspections_2y,
  ktc_trade_inspections_temp.avg_deficiencies_per_inspection_2y,
  ktc_trade_inspections_temp.avg_detentions_per_inspection_2y,
  ktc_trade_inspections_temp.avg_n_detentions_2y
FROM
  kpler_trade
  LEFT OUTER JOIN kpler_product ON kpler_trade.product_id = kpler_product.id
  LEFT OUTER JOIN kpler_zone AS kpler_zone_1 ON kpler_trade.departure_zone_id = kpler_zone_1.id
  LEFT OUTER JOIN kpler_zone AS kpler_zone_2 ON kpler_trade.arrival_zone_id = kpler_zone_2.id
  LEFT OUTER JOIN commodity ON 'kpler_' || replace(
    replace(
      lower(
        coalesce(
          kpler_product.commodity_name,
          kpler_product.group_name
        )
      ),
      ' ',
      '_'
    ),
    '/',
    '_'
  ) = commodity.id
  LEFT OUTER JOIN commodity AS commodity_1 ON commodity.equivalent_id = commodity_1.id
  LEFT OUTER JOIN ktc_trade_price_temp ON kpler_trade.id = ktc_trade_price_temp.trade_id
  AND kpler_trade.flow_id = ktc_trade_price_temp.flow_id
  LEFT OUTER JOIN price ON ktc_trade_price_temp.price_id = price.id
  LEFT OUTER JOIN ktc_trade_designations_temp ON kpler_trade.id = ktc_trade_designations_temp.trade_id
  AND kpler_trade.flow_id = ktc_trade_designations_temp.flow_id
  LEFT OUTER JOIN ktc_trade_step_zones_temp ON kpler_trade.id = ktc_trade_step_zones_temp.trade_id
  AND kpler_trade.flow_id = ktc_trade_step_zones_temp.flow_id
  LEFT OUTER JOIN ktc_vessel_ages_for_trade_temp ON kpler_trade.id = ktc_vessel_ages_for_trade_temp.trade_id
  AND kpler_trade.flow_id = ktc_vessel_ages_for_trade_temp.flow_id
  LEFT OUTER JOIN ktc_trade_largest_vessel_temp ON kpler_trade.id = ktc_trade_largest_vessel_temp.trade_id
  AND kpler_trade.flow_id = ktc_trade_largest_vessel_temp.flow_id
  LEFT OUTER JOIN ktc_trade_vessel_types_temp ON kpler_trade.id = ktc_trade_vessel_types_temp.trade_id
  AND kpler_trade.flow_id = ktc_trade_vessel_types_temp.flow_id
  LEFT OUTER JOIN ktc_trade_inspections_temp ON kpler_trade.id = ktc_trade_inspections_temp.trade_id
  AND kpler_trade.flow_id = ktc_trade_inspections_temp.flow_id
WHERE
  kpler_trade.is_valid
  AND price.scenario IS NOT NULL
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  kpler_trade.product_id,
  price.scenario;

CREATE INDEX ON ktc_kpler_trade_computed_temp (trade_id);

CREATE INDEX ON ktc_kpler_trade_computed_temp (flow_id);

CREATE INDEX ON ktc_kpler_trade_computed_temp (ownership_sanction_coverage);

CREATE INDEX ON ktc_kpler_trade_computed_temp (pricing_scenario);

CREATE INDEX ON ktc_kpler_trade_computed_temp (pricing_commodity);

CREATE INDEX ON ktc_kpler_trade_computed_temp (kpler_product_commodity_id);

ANALYZE ktc_kpler_trade_computed_temp;
