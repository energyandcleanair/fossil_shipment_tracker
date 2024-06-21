CREATE MATERIALIZED VIEW ktc_voyage_insurer AS
SELECT
  DISTINCT ON (
    kpler_trade.id,
    kpler_trade.flow_id,
    ktc_trade_ship.ship_imo
  ) kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  ktc_trade_ship.ship_imo AS ship_imo,
  coalesce(company.name, 'unknown') AS name,
  coalesce(company.country_iso2, 'unknown') AS iso2,
  coalesce(country.region, 'unknown') AS region,
  'PCC' = any(country.regions) AS in_pcc,
  country.iso2 = 'NO' AS in_norway
FROM
  kpler_trade
  JOIN ktc_trade_ship ON kpler_trade.id = ktc_trade_ship.trade_id
  AND kpler_trade.flow_id = ktc_trade_ship.flow_id
  LEFT JOIN ship_insurer ON ship_insurer.ship_imo = ktc_trade_ship.ship_imo
  AND ship_insurer.is_valid = true
  AND (
    coalesce(
      ship_insurer.date_from_insurer,
      ship_insurer.date_from_equasis
    ) <= kpler_trade.departure_date_utc + INTERVAL '14 days'
    OR coalesce(
      ship_insurer.date_from_insurer,
      ship_insurer.date_from_equasis
    ) IS NULL
  )
  LEFT JOIN company ON ship_insurer.company_id = company.id
  LEFT JOIN country ON company.country_iso2 = country.iso2
WHERE
  kpler_trade.is_valid
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  ktc_trade_ship.ship_imo,
  coalesce(
    ship_insurer.date_from_insurer,
    ship_insurer.date_from_equasis
  ) DESC NULLS LAST,
  ship_insurer.updated_on DESC NULLS LAST;

CREATE INDEX ON ktc_voyage_insurer (trade_id);

CREATE INDEX ON ktc_voyage_insurer (flow_id);

CREATE INDEX ON ktc_voyage_insurer (ship_imo);

CREATE INDEX ON ktc_voyage_insurer (iso2);

ANALYZE ktc_voyage_insurer;
