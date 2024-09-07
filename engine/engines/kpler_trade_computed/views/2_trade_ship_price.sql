CREATE MATERIALIZED VIEW ktc_trade_ship_price_temp AS
SELECT
  DISTINCT ON (
    kpler_trade.id,
    kpler_trade.flow_id,
    ktc_trade_ship_temp.ship_order,
    ktc_trade_ship_temp.ship_imo,
    price.scenario
  ) kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  ktc_trade_ship_temp.ship_imo AS ship_imo,
  price.scenario AS scenario,
  price.id AS price_id
FROM
  kpler_trade
  JOIN ktc_trade_ship_temp ON (
    kpler_trade.id = ktc_trade_ship_temp.trade_id
    AND kpler_trade.flow_id = ktc_trade_ship_temp.flow_id
  )
  JOIN ktc_trade_commodity_temp ON (
    kpler_trade.id = ktc_trade_commodity_temp.trade_id
    AND kpler_trade.flow_id = ktc_trade_commodity_temp.flow_id
  )
  LEFT OUTER JOIN kpler_zone AS departure_zone ON kpler_trade.departure_zone_id = departure_zone.id
  LEFT OUTER JOIN kpler_zone AS arrival_zone ON kpler_trade.arrival_zone_id = arrival_zone.id
  LEFT OUTER JOIN ktc_voyage_insurer_temp ON (
    ktc_voyage_insurer_temp.trade_id = kpler_trade.id
    AND ktc_voyage_insurer_temp.flow_id = kpler_trade.flow_id
    AND ktc_voyage_insurer_temp.ship_order = ktc_trade_ship_temp.ship_order
    AND ktc_voyage_insurer_temp.ship_imo = ktc_trade_ship_temp.ship_imo
  )
  LEFT OUTER JOIN ktc_voyage_owner_temp ON (
    ktc_voyage_owner_temp.trade_id = kpler_trade.id
    AND ktc_voyage_owner_temp.flow_id = kpler_trade.flow_id
    AND ktc_voyage_owner_temp.ship_order = ktc_trade_ship_temp.ship_order
    AND ktc_voyage_owner_temp.ship_imo = ktc_trade_ship_temp.ship_imo
  )
  JOIN price ON (
    price.date = date_trunc('day', kpler_trade.departure_date_utc)
    AND price.commodity = ktc_trade_commodity_temp.pricing_commodity
    AND (
      (
        departure_zone.country_iso2 = 'RU'
        AND arrival_zone.country_iso2 = ANY(price.destination_iso2s)
      )
      OR price.destination_iso2s = ARRAY [NULL :: VARCHAR]
    )
    AND (
      (
        departure_zone.country_iso2 = 'RU'
        AND ktc_voyage_insurer_temp.iso2 = ANY(price.ship_insurer_iso2s)
      )
      OR price.ship_insurer_iso2s = ARRAY [NULL :: VARCHAR]
    )
    AND (
      (
        departure_zone.country_iso2 = 'RU'
        AND ktc_voyage_owner_temp.iso2 = ANY(price.ship_owner_iso2s)
      )
      OR price.ship_owner_iso2s = ARRAY [NULL :: VARCHAR]
    )
  )
WHERE
  kpler_trade.is_valid
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id,
  ktc_trade_ship_temp.ship_order,
  ktc_trade_ship_temp.ship_imo,
  price.scenario,
  price.destination_iso2s,
  price.ship_insurer_iso2s,
  price.ship_owner_iso2s;

CREATE INDEX ON ktc_trade_ship_price_temp (trade_id);

CREATE INDEX ON ktc_trade_ship_price_temp (flow_id);

CREATE INDEX ON ktc_trade_ship_price_temp (ship_imo);

CREATE INDEX ON ktc_trade_ship_price_temp (price_id);

ANALYZE ktc_trade_ship_price_temp;
