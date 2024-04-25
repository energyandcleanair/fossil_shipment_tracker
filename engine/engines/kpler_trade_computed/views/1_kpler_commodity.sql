CREATE MATERIALIZED VIEW ktc_kpler_commodity AS
SELECT
  kpler_product.id AS product_id,
  'kpler_' || replace(
    replace(
      lower(
        coalesce(
          kpler_product.commodity_name,
          kpler_product.group_name
        )
      ),
      ' ',
      '_'
    ),
    '/',
    '_'
  ) AS commodity_id
FROM
  kpler_product;

CREATE INDEX ON ktc_kpler_commodity (product_id);

CREATE INDEX ON ktc_kpler_commodity (commodity_id);
