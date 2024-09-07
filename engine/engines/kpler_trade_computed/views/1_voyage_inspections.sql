CREATE MATERIALIZED VIEW ktc_voyage_inspections_temp AS
SELECT
    ktc_trade_ship_temp.trade_id,
    ktc_trade_ship_temp.flow_id,
    ktc_trade_ship_temp.ship_order,
    ktc_trade_ship_temp.ship_imo,
    COUNT(ship_inspection.id) AS n_inspections_2y,
    AVG(
        COALESCE(ship_inspection.number_of_deficiencies, 0)
    ) AS deficiencies_per_inspection_2y,
    AVG(
        CASE
            WHEN ship_inspection.detention THEN 1
            ELSE 0
        END
    ) AS detentions_per_inspection_2y,
    SUM(
        CASE
            WHEN ship_inspection.detention THEN 1
            ELSE 0
        END
    ) AS n_detentions_2y
FROM
    ktc_trade_ship_temp
    LEFT JOIN kpler_trade ON ktc_trade_ship_temp.trade_id = kpler_trade.id
    AND ktc_trade_ship_temp.flow_id = kpler_trade.flow_id
    LEFT JOIN ship_inspection ON ship_inspection.ship_imo = ktc_trade_ship_temp.ship_imo
    AND ship_inspection.date_of_report >= kpler_trade.departure_date_utc - INTERVAL '2 years'
    AND ship_inspection.date_of_report <= kpler_trade.departure_date_utc
GROUP BY
    ktc_trade_ship_temp.trade_id,
    ktc_trade_ship_temp.flow_id,
    ktc_trade_ship_temp.ship_order,
    ktc_trade_ship_temp.ship_imo;

CREATE INDEX ON ktc_voyage_inspections_temp (trade_id);

CREATE INDEX ON ktc_voyage_inspections_temp (flow_id);

CREATE INDEX ON ktc_voyage_inspections_temp (ship_order);

CREATE INDEX ON ktc_voyage_inspections_temp (ship_imo);

ANALYZE ktc_voyage_inspections_temp;
