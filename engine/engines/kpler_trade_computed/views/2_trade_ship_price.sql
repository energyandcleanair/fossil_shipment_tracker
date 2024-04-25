CREATE MATERIALIZED VIEW ktc_trade_ship_price AS
SELECT
  DISTINCT ON (
    kpler_trade.id,
    kpler_trade.flow_id,
    ktc_trade_ship.ship_imo,
    price.scenario
  ) kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  ktc_trade_ship.ship_imo AS ship_imo,
  price.scenario AS scenario,
  price.id AS price_id
FROM
  kpler_trade
  JOIN ktc_trade_ship ON (
    kpler_trade.id = ktc_trade_ship.trade_id
    AND kpler_trade.flow_id = ktc_trade_ship.flow_id
  )
  JOIN ktc_trade_commodity ON (
    kpler_trade.id = ktc_trade_commodity.trade_id
    AND kpler_trade.flow_id = ktc_trade_commodity.flow_id
  )
  LEFT OUTER JOIN kpler_zone AS departure_zone ON kpler_trade.departure_zone_id = departure_zone.id
  LEFT OUTER JOIN kpler_zone AS arrival_zone ON kpler_trade.arrival_zone_id = arrival_zone.id
  LEFT OUTER JOIN ktc_voyage_insurer ON (
    ktc_voyage_insurer.trade_id = kpler_trade.id
    AND ktc_voyage_insurer.flow_id = kpler_trade.flow_id
    AND ktc_voyage_insurer.ship_imo = ktc_trade_ship.ship_imo
  )
  LEFT OUTER JOIN ktc_voyage_owner ON (
    ktc_voyage_owner.trade_id = kpler_trade.id
    AND ktc_voyage_owner.flow_id = kpler_trade.flow_id
    AND ktc_voyage_owner.ship_imo = ktc_trade_ship.ship_imo
  )
  JOIN price ON (
    price.date = date_trunc('day', kpler_trade.departure_date_utc)
    AND price.commodity = ktc_trade_commodity.pricing_commodity
  )
  JOIN ktc_price_destination_iso2 ON (
    --- This will filter down the prices to matching destination_iso2s or the default price if there isn't one.
    price.id = ktc_price_destination_iso2.id
    AND (
      (
        departure_zone.country_iso2 = 'RU'
        AND ktc_price_destination_iso2.destination_iso2 = arrival_zone.country_iso2
      )
      OR ktc_price_destination_iso2.destination_iso2 IS NULL
    )
  )
  JOIN ktc_price_ship_insurer_iso2 ON (
    --- This will filter down the prices to matching ship_insurer_iso2s or the default price value if there isn't one.
    price.id = ktc_price_ship_insurer_iso2.id
    AND (
      (
        departure_zone.country_iso2 = 'RU'
        AND ktc_price_ship_insurer_iso2.ship_insurer_iso2 = ktc_voyage_insurer.iso2
      )
      OR ktc_price_ship_insurer_iso2.ship_insurer_iso2 IS NULL
    )
  )
  JOIN ktc_price_ship_owner_iso2 ON (
    --- This will filter down the prices to matching ship_owner_iso2s or the default price if there isn't one.
    price.id = ktc_price_ship_owner_iso2.id
    AND (
      (
        departure_zone.country_iso2 = 'RU'
        AND ktc_price_ship_owner_iso2.ship_owner_iso2 = ktc_voyage_owner.iso2
      )
      OR ktc_price_ship_owner_iso2.ship_owner_iso2 IS NULL
    )
  )
WHERE
  kpler_trade.is_valid
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  ktc_trade_ship.ship_imo,
  price.scenario,
  price.destination_iso2s,
  price.ship_insurer_iso2s,
  price.ship_owner_iso2s;

CREATE INDEX ON ktc_trade_ship_price (trade_id);

CREATE INDEX ON ktc_trade_ship_price (flow_id);

CREATE INDEX ON ktc_trade_ship_price (ship_imo);

CREATE INDEX ON ktc_trade_ship_price (price_id);
