CREATE MATERIALIZED VIEW ktc_vessel_ages_for_trade AS
SELECT
  kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  array_agg(
    ktc_vessel_age.vessel_age
    ORDER BY
      ktc_vessel_age.ship_order
  ) AS vessel_ages,
  avg(ktc_vessel_age.vessel_age) AS avg_vessel_age
FROM
  kpler_trade
  LEFT OUTER JOIN ktc_vessel_age ON kpler_trade.id = ktc_vessel_age.trade_id
  AND kpler_trade.flow_id = ktc_vessel_age.flow_id
WHERE
  kpler_trade.is_valid
GROUP BY
  kpler_trade.id,
  kpler_trade.flow_id
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id;

CREATE INDEX ON ktc_vessel_ages_for_trade (trade_id);

CREATE INDEX ON ktc_vessel_ages_for_trade (flow_id);

ANALYZE ktc_vessel_ages_for_trade;
