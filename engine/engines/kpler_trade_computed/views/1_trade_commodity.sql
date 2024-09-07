CREATE MATERIALIZED VIEW ktc_trade_commodity_temp AS
SELECT
  kpler_trade.id AS trade_id,
  kpler_trade.flow_id AS flow_id,
  CASE
    WHEN (
      kpler_product.group_name = 'Crude/Co'
      AND departure_zone.country_iso2 = 'RU'
      AND (
        kpler_product.grade_name NOT IN ('CPC Kazakhstan', 'KEBCO')
      )
      AND (
        departure_zone.port_name ~* '^Nakhodka|^De Kast|^Prigorod'
      )
    ) THEN 'crude_oil_espo'
    WHEN (
      kpler_product.group_name = 'Crude/Co'
      AND departure_zone.country_iso2 = 'RU'
      AND (
        kpler_product.grade_name NOT IN ('CPC Kazakhstan', 'KEBCO')
      )
    ) THEN 'crude_oil_urals'
    ELSE commodity.pricing_commodity
  END AS pricing_commodity
FROM
  kpler_trade
  JOIN kpler_product ON kpler_trade.product_id = kpler_product.id
  LEFT OUTER JOIN ktc_kpler_commodity_temp ON ktc_kpler_commodity_temp.product_id = kpler_product.id
  LEFT OUTER JOIN commodity ON ktc_kpler_commodity_temp.commodity_id = commodity.id
  LEFT OUTER JOIN kpler_zone AS departure_zone ON kpler_trade.departure_zone_id = departure_zone.id
WHERE
  kpler_trade.is_valid
ORDER BY
  kpler_trade.id,
  kpler_trade.flow_id;

CREATE INDEX ON ktc_trade_commodity_temp (trade_id);

CREATE INDEX ON ktc_trade_commodity_temp (flow_id);

CREATE INDEX ON ktc_trade_commodity_temp (pricing_commodity);

ANALYZE ktc_trade_commodity_temp;
