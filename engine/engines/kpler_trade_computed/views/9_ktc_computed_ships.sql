CREATE MATERIALIZED VIEW ktc_kpler_trade_computed_ships AS
SELECT
    ktc_kpler_trade_computed.trade_id,
    ktc_kpler_trade_computed.flow_id,
    ktc_kpler_trade_computed.product_id,
    ktc_kpler_trade_computed.pricing_scenario,
    ktc_kpler_trade_computed.ownership_sanction_coverage,
    ktc_kpler_trade_computed.pricing_commodity,
    ktc_kpler_trade_computed.kpler_product_commodity_id,
    ktc_kpler_trade_computed.flag_sanction_coverage,
    ktc_kpler_trade_computed.eur_per_tonne,
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
    array_length(ktc_kpler_trade_computed.vessel_imos, 1) AS total_steps_in_trade,
    ships.vessel_type,
    ships.vessel_capacity_cm
FROM
    ktc_kpler_trade_computed,
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
        vessel_capacities_cm
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
        step_in_trade
    );

CREATE INDEX ON ktc_kpler_trade_computed_ships (trade_id);

CREATE INDEX ON ktc_kpler_trade_computed_ships (flow_id);

ANALYZE ktc_kpler_trade_computed_ships;
