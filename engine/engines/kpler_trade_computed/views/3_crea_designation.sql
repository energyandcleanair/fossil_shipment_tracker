CREATE MATERIALIZED VIEW ktc_crea_designation AS
SELECT
    percent.trade_id,
    percent.flow_id,
    percent.ship_order,
    percent.ship_imo,
    (
        CASE
            WHEN (
                ktc_voyage_owner.in_pcc
                OR ktc_voyage_owner.in_norway
                OR ktc_voyage_insurer.in_pcc
                OR ktc_voyage_insurer.in_norway
                OR ktc_voyage_flag.in_pcc
                OR ktc_voyage_flag.in_norway
            ) THEN 'G7+'
            WHEN percent.recent_ru_percent = 1 THEN 'Shadow'
            ELSE 'Taxi'
        END
    ) :: VARCHAR AS crea_designation
FROM
    ktc_recent_ru_trade_percent percent
    LEFT JOIN ktc_voyage_insurer ON percent.trade_id = ktc_voyage_insurer.trade_id
    AND percent.flow_id = ktc_voyage_insurer.flow_id
    AND percent.ship_order = ktc_voyage_insurer.ship_order
    AND percent.ship_imo = ktc_voyage_insurer.ship_imo
    LEFT JOIN ktc_voyage_owner ON percent.trade_id = ktc_voyage_owner.trade_id
    AND percent.flow_id = ktc_voyage_owner.flow_id
    AND percent.ship_order = ktc_voyage_owner.ship_order
    AND percent.ship_imo = ktc_voyage_owner.ship_imo
    LEFT JOIN ktc_voyage_flag ON percent.trade_id = ktc_voyage_flag.trade_id
    AND percent.flow_id = ktc_voyage_flag.flow_id
    AND percent.ship_order = ktc_voyage_flag.ship_order
    and percent.ship_imo = ktc_voyage_flag.ship_imo
ORDER BY
    percent.trade_id,
    percent.flow_id,
    percent.ship_order;

CREATE INDEX ON ktc_crea_designation (trade_id);

CREATE INDEX ON ktc_crea_designation (flow_id);

CREATE INDEX ON ktc_crea_designation (ship_imo);

ANALYZE ktc_crea_designation;
