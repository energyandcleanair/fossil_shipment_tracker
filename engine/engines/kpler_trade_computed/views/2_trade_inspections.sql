-- Create an array of the inspection (from ktc_voyage_inspections) for the trade and the average
-- for each stat across the whole trade.
CREATE MATERIALIZED VIEW ktc_trade_inspections AS
SELECT
    trade_id,
    flow_id,
    array_agg(
        n_inspections_2y
        ORDER BY
            ship_order
    ) AS n_inspections_2y,
    array_agg(
        deficiencies_per_inspection_2y
        ORDER BY
            ship_order
    ) AS deficiencies_per_inspection_2y,
    array_agg(
        detentions_per_inspection_2y
        ORDER BY
            ship_order
    ) AS detentions_per_inspection_2y,
    AVG(n_inspections_2y) AS avg_n_inspections_2y,
    AVG(deficiencies_per_inspection_2y) AS avg_deficiencies_per_inspection_2y,
    AVG(detentions_per_inspection_2y) AS avg_detentions_per_inspection_2y
FROM
    ktc_voyage_inspections
GROUP BY
    trade_id,
    flow_id;

CREATE INDEX ON ktc_trade_inspections (trade_id);

CREATE INDEX ON ktc_trade_inspections (flow_id);

ANALYZE ktc_trade_inspections;
