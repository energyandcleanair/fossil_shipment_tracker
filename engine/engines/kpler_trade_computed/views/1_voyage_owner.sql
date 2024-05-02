CREATE MATERIALIZED VIEW ktc_voyage_owner AS
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
  coalesce(country.region, 'unknown') AS region
FROM
  kpler_trade
  JOIN ktc_trade_ship ON kpler_trade.id = ktc_trade_ship.trade_id
  AND kpler_trade.flow_id = ktc_trade_ship.flow_id
  LEFT OUTER JOIN ship_owner ON ship_owner.ship_imo = ktc_trade_ship.ship_imo
  AND (
    ship_owner.date_from <= kpler_trade.departure_date_utc + INTERVAL '14 days'
    OR ship_owner.date_from IS NULL
  )
  LEFT OUTER JOIN company ON ship_owner.company_id = company.id
  LEFT OUTER JOIN country ON company.country_iso2 = country.iso2
WHERE
  kpler_trade.is_valid
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  ktc_trade_ship.ship_imo,
  ship_owner.date_from DESC NULLS LAST,
  ship_owner.updated_on DESC NULLS LAST;

CREATE INDEX ON ktc_voyage_owner (trade_id);

CREATE INDEX ON ktc_voyage_owner (flow_id);

CREATE INDEX ON ktc_voyage_owner (ship_imo);

CREATE INDEX ON ktc_voyage_owner (iso2);

ANALYZE ktc_voyage_owner;
