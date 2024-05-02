CREATE MATERIALIZED VIEW ktc_kpler_trade_computed AS
SELECT
  kpler_trade.id AS trade_id,
  kpler_trade.flow_id,
  kpler_trade.product_id,
  commodity.id AS kpler_product_commodity_id,
  price.scenario AS pricing_scenario,
  price.commodity AS pricing_commodity,
  price.eur_per_tonne,
  ktc_insurers_and_owners_for_trade.ship_insurer_names,
  ktc_insurers_and_owners_for_trade.ship_insurer_iso2s,
  ktc_insurers_and_owners_for_trade.ship_insurer_regions,
  ktc_insurers_and_owners_for_trade.ship_owner_names,
  ktc_insurers_and_owners_for_trade.ship_owner_iso2s,
  ktc_insurers_and_owners_for_trade.ship_owner_regions,
  CASE
    WHEN (
      ktc_insurers_and_owners_for_trade.insured_in_pcc
      OR ktc_insurers_and_owners_for_trade.owned_in_pcc
    ) THEN 'Owned and / or insured in EU & G7'
    WHEN ktc_insurers_and_owners_for_trade.insured_in_norway THEN 'Insured in Norway'
    WHEN ktc_insurers_and_owners_for_trade.owner_known THEN 'Others'
    ELSE 'Unknown'
  END AS ownership_sanction_coverage,
  coalesce(
    ktc_trade_step_zones.step_zone_names,
    ARRAY [] :: varchar []
  ) AS step_zone_names,
  coalesce(
    ktc_trade_step_zones.step_zone_iso2s,
    ARRAY [] :: varchar []
  ) AS step_zone_iso2s,
  coalesce(
    ktc_trade_step_zones.step_zone_regions,
    ARRAY [] :: varchar []
  ) AS step_zone_regions,
  coalesce(
    kpler_trade.step_zone_ids,
    ARRAY [] :: numeric []
  ) AS step_zone_ids,
  ktc_vessel_ages_for_trade.vessel_ages,
  ktc_vessel_ages_for_trade.avg_vessel_age
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
  LEFT OUTER JOIN ktc_trade_price ON kpler_trade.id = ktc_trade_price.trade_id
  AND kpler_trade.flow_id = ktc_trade_price.flow_id
  LEFT OUTER JOIN price ON ktc_trade_price.price_id = price.id
  LEFT OUTER JOIN ktc_insurers_and_owners_for_trade ON kpler_trade.id = ktc_insurers_and_owners_for_trade.trade_id
  AND kpler_trade.flow_id = ktc_insurers_and_owners_for_trade.flow_id
  LEFT OUTER JOIN ktc_trade_step_zones ON kpler_trade.id = ktc_trade_step_zones.trade_id
  AND kpler_trade.flow_id = ktc_trade_step_zones.flow_id
  LEFT OUTER JOIN ktc_vessel_ages_for_trade ON kpler_trade.id = ktc_vessel_ages_for_trade.trade_id
  AND kpler_trade.flow_id = ktc_vessel_ages_for_trade.flow_id
WHERE
  kpler_trade.is_valid
  AND price.scenario IS NOT NULL
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  kpler_trade.product_id,
  price.scenario;

CREATE INDEX ON ktc_kpler_trade_computed (trade_id);

CREATE INDEX ON ktc_kpler_trade_computed (flow_id);

ANALYZE ktc_kpler_trade_computed;
