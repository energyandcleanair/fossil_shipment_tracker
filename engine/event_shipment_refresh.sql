WITH inserted_shipment_event AS (
  INSERT INTO
    event_shipment (shipment_id, event_id, created_at)
  SELECT DISTINCT ON (all_shipment_events.shipment_id, all_shipment_events.ship_name, all_shipment_events.interacting_ship_name)
    all_shipment_events.shipment_id,
    all_shipment_events.event_id,
    all_shipment_events.created_at
  FROM
    (
    SELECT
        lead_shipment.id AS shipment_id,
        ev.id AS event_id,
        timezone('utc', NOW()) AS created_at,
        ev.ship_name,
        ev.interacting_ship_name,
        ev.distance_meters
  FROM
    (
      SELECT
        shipment.id,
        departure.ship_imo,
        departure.date_utc AS departure_date,
        arrival.date_utc AS arrival_date,
        shipment.status,
        lead(departure.date_utc) over (
          PARTITION BY departure.ship_imo
          ORDER BY
            departure.ship_imo,
            departure.date_utc ASC
        ) AS next_departure_date_utc
      FROM
        shipment
        LEFT JOIN departure ON departure.id = shipment.departure_id
        LEFT JOIN arrival ON arrival.id = shipment.arrival_id
      WHERE
        departure.date_utc > '2021-11-01'
      ORDER BY
        departure.ship_imo,
        departure.date_utc ASC
    ) AS lead_shipment
    LEFT JOIN (
      SELECT
        event.id,
        event.ship_imo,
        event.date_utc,
        event.ship_name,
        event.interacting_ship_name,
        CAST(event.interacting_ship_details->>'distance_meters' AS DOUBLE PRECISION) AS distance_meters
      FROM
        event
      WHERE
        event.type_id = '21'
    ) AS ev ON (
      ev.ship_imo = lead_shipment.ship_imo
      AND (
        (
          lead_shipment.next_departure_date_utc IS NOT NULL
          AND ev.date_utc BETWEEN lead_shipment.departure_date
          AND lead_shipment.next_departure_date_utc
        )
        OR (
          lead_shipment.next_departure_date_utc IS NULL
          AND ev.date_utc BETWEEN lead_shipment.departure_date
          AND lead_shipment.arrival_date + INTERVAL '1 hour' * 72
        )
      )
    )
  WHERE
    ev.id IS NOT NULL
    ) AS all_shipment_events
  ORDER BY all_shipment_events.shipment_id,
           all_shipment_events.ship_name,
           all_shipment_events.interacting_ship_name,
           all_shipment_events.distance_meters ASC
  ON CONFLICT
    DO NOTHING
  RETURNING
    shipment_id,
    event_id,
    created_at
)
SELECT
  count(event_id)
FROM
  inserted_shipment_event;