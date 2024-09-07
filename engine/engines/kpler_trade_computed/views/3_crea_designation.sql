CREATE MATERIALIZED VIEW ktc_crea_designation_temp AS
SELECT
    percent.trade_id,
    percent.flow_id,
    percent.ship_order,
    percent.ship_imo,
    (
        CASE
            WHEN (
                ktc_voyage_owner_temp.in_pcc
                OR ktc_voyage_owner_temp.in_norway
                OR ktc_voyage_insurer_temp.in_pcc
                OR ktc_voyage_insurer_temp.in_norway
                OR ktc_voyage_flag_temp.in_pcc
                OR ktc_voyage_flag_temp.in_norway
            ) THEN 'G7+'
            WHEN percent.recent_ru_percent = 1 THEN 'Shadow'
            ELSE 'Taxi'
        END
    ) :: VARCHAR AS crea_designation
FROM
    ktc_recent_ru_trade_percent_temp percent
    LEFT JOIN ktc_voyage_insurer_temp ON percent.trade_id = ktc_voyage_insurer_temp.trade_id
    AND percent.flow_id = ktc_voyage_insurer_temp.flow_id
    AND percent.ship_order = ktc_voyage_insurer_temp.ship_order
    AND percent.ship_imo = ktc_voyage_insurer_temp.ship_imo
    LEFT JOIN ktc_voyage_owner_temp ON percent.trade_id = ktc_voyage_owner_temp.trade_id
    AND percent.flow_id = ktc_voyage_owner_temp.flow_id
    AND percent.ship_order = ktc_voyage_owner_temp.ship_order
    AND percent.ship_imo = ktc_voyage_owner_temp.ship_imo
    LEFT JOIN ktc_voyage_flag_temp ON percent.trade_id = ktc_voyage_flag_temp.trade_id
    AND percent.flow_id = ktc_voyage_flag_temp.flow_id
    AND percent.ship_order = ktc_voyage_flag_temp.ship_order
    and percent.ship_imo = ktc_voyage_flag_temp.ship_imo
ORDER BY
    percent.trade_id,
    percent.flow_id,
    percent.ship_order;

CREATE INDEX ON ktc_crea_designation_temp (trade_id);

CREATE INDEX ON ktc_crea_designation_temp (flow_id);

CREATE INDEX ON ktc_crea_designation_temp (ship_imo);

ANALYZE ktc_crea_designation_temp;
