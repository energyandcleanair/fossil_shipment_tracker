DELETE FROM event_shipment
WHERE id IN (
    SELECT DISTINCT(id)
    FROM event_shipment
    WHERE event_id IS NOT NULL
)
AND event_id IS NULL;
SELECT count(id)
FROM event_shipment
WHERE event_id IS NOT NULL;