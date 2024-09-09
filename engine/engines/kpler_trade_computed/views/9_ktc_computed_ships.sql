CREATE MATERIALIZED VIEW ktc_kpler_trade_computed_ships_temp AS WITH unnested_ktc_kpler_trade_computed AS (
    SELECT
        ktc_kpler_trade_computed_temp.trade_id,
        ktc_kpler_trade_computed_temp.flow_id,
        ktc_kpler_trade_computed_temp.product_id,
        ktc_kpler_trade_computed_temp.pricing_scenario,
        ktc_kpler_trade_computed_temp.ownership_sanction_coverage,
        ktc_kpler_trade_computed_temp.pricing_commodity,
        ktc_kpler_trade_computed_temp.kpler_product_commodity_id,
        ktc_kpler_trade_computed_temp.flag_sanction_coverage,
        ktc_kpler_trade_computed_temp.eur_per_tonne,
        coalesce(ships.vessel_imo, 'unknown') AS vessel_imo,
        ships.ship_insurer_name,
        ships.ship_insurer_iso2,
        ships.ship_insurer_region,
        ships.ship_owner_name,
        ships.ship_owner_iso2,
        ships.ship_owner_region,
        ships.vessel_age,
        ships.ship_flag_iso2,
        ships.crea_designation,
        ships.step_in_trade,
        array_length(ktc_kpler_trade_computed_temp.vessel_imos, 1) AS total_steps_in_trade,
        ships.vessel_type,
        ships.vessel_capacity_cm,
        ships.n_inspections_2y,
        ships.deficiencies_per_inspection_2y,
        ships.detentions_per_inspection_2y,
        ships.n_detentions_2y
    FROM
        ktc_kpler_trade_computed_temp,
        unnest(
            vessel_imos,
            ship_insurer_names,
            ship_insurer_iso2s,
            ship_insurer_regions,
            ship_owner_names,
            ship_owner_iso2s,
            ship_owner_regions,
            vessel_ages,
            ship_flag_iso2s,
            crea_designations,
            vessel_types,
            vessel_capacities_cm,
            n_inspections_2y,
            deficiencies_per_inspection_2y,
            detentions_per_inspection_2y,
            n_detentions_2y
        ) WITH ORDINALITY AS ships(
            vessel_imo,
            ship_insurer_name,
            ship_insurer_iso2,
            ship_insurer_region,
            ship_owner_name,
            ship_owner_iso2,
            ship_owner_region,
            vessel_age,
            ship_flag_iso2,
            crea_designation,
            vessel_type,
            vessel_capacity_cm,
            n_inspections_2y,
            deficiencies_per_inspection_2y,
            detentions_per_inspection_2y,
            n_detentions_2y,
            step_in_trade
        )
)
SELECT
    unnested_ktc_kpler_trade_computed.*,
    ktc_voyage_sts_temp.start_sts_zone_id,
    ktc_voyage_sts_temp.start_sts_zone_name,
    ktc_voyage_sts_temp.start_sts_iso2,
    ktc_voyage_sts_temp.start_sts_region,
    ktc_voyage_sts_temp.end_sts_zone_id,
    ktc_voyage_sts_temp.end_sts_zone_name,
    ktc_voyage_sts_temp.end_sts_iso2,
    ktc_voyage_sts_temp.end_sts_region
FROM
    unnested_ktc_kpler_trade_computed
    LEFT OUTER JOIN ktc_voyage_sts_temp ON unnested_ktc_kpler_trade_computed.trade_id = ktc_voyage_sts_temp.trade_id
    AND unnested_ktc_kpler_trade_computed.flow_id = ktc_voyage_sts_temp.flow_id
    AND unnested_ktc_kpler_trade_computed.vessel_imo = ktc_voyage_sts_temp.ship_imo
    AND unnested_ktc_kpler_trade_computed.step_in_trade = ktc_voyage_sts_temp.ship_order;

CREATE INDEX ON ktc_kpler_trade_computed_ships_temp (trade_id);

CREATE INDEX ON ktc_kpler_trade_computed_ships_temp (flow_id);

CREATE INDEX ON ktc_kpler_trade_computed_ships_temp (product_id);

CREATE INDEX ON ktc_kpler_trade_computed_ships_temp (pricing_scenario);

CREATE INDEX ON ktc_kpler_trade_computed_ships_temp (vessel_imo);

ANALYZE ktc_kpler_trade_computed_ships_temp;
