WITH inserted_shipment_event AS (
INSERT INTO event_shipment (shipment_id, event_id, created_at)
SELECT
	lead_shipment.id AS shipment_id,
	ev.id AS event_id,
	timezone('utc', now()) as created_at
	FROM
	(SELECT
			shipment.id,
			departure.ship_imo,
			departure.date_utc AS departure_date,
			arrival.date_utc AS arrival_date,
			shipment.status,
			lead(departure.date_utc) over (
				partition BY departure.ship_imo
				ORDER BY departure.date_utc
			) AS next_departure_date_utc
	FROM shipment
	LEFT JOIN departure ON departure.id = shipment.departure_id
	LEFT JOIN arrival ON arrival.id = shipment.arrival_id
	WHERE departure.date_utc > '2021-11-01'
	ORDER BY departure.ship_imo, departure.date_utc ASC) AS lead_shipment
 	LEFT JOIN (SELECT event.id, event.ship_imo, event.date_utc
		   FROM event
		   WHERE event.type_id = '21') AS ev
		   ON ((ev.date_utc BETWEEN lead_shipment.departure_date AND lead_shipment.next_departure_date_utc)
		   AND ev.ship_imo = lead_shipment.ship_imo)
    ON CONFLICT
        DO NOTHING
    RETURNING
        shipment_id,
        event_id,
        created_at
)
SELECT count(event_id)
FROM inserted_shipment_event;