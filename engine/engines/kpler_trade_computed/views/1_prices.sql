CREATE MATERIALIZED VIEW ktc_price_destination_iso2 AS
SELECT
  id,
  unnest(destination_iso2s) AS destination_iso2
FROM
  price;

CREATE INDEX ON ktc_price_destination_iso2 (id);

CREATE INDEX ON ktc_price_destination_iso2 (destination_iso2);

CREATE MATERIALIZED VIEW ktc_price_departure_port_id AS
SELECT
  id,
  unnest(departure_port_ids) AS departure_port_id
FROM
  price;

CREATE INDEX ON ktc_price_departure_port_id (id);

CREATE INDEX ON ktc_price_departure_port_id (departure_port_id);

CREATE MATERIALIZED VIEW ktc_price_ship_owner_iso2 AS
SELECT
  id,
  unnest(ship_owner_iso2s) AS ship_owner_iso2
FROM
  price;

CREATE INDEX ON ktc_price_ship_owner_iso2 (id);

CREATE INDEX ON ktc_price_ship_owner_iso2 (ship_owner_iso2);

CREATE MATERIALIZED VIEW ktc_price_ship_insurer_iso2 AS
SELECT
  id,
  unnest(ship_insurer_iso2s) AS ship_insurer_iso2
FROM
  price;

CREATE INDEX ON ktc_price_ship_insurer_iso2 (id);

CREATE INDEX ON ktc_price_ship_insurer_iso2 (ship_insurer_iso2);
