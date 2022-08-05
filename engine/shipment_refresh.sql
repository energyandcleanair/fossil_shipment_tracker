WITH completed_departure_portcalls AS (
    SELECT
        departure.portcall_id AS id
    FROM
        shipment
        LEFT JOIN departure ON shipment.departure_id = departure.id
    WHERE
        shipment.status = 'completed'
),
completed_arrival_portcalls AS (
    SELECT
        portcall_id as id
    FROM
        arrival
    WHERE
        portcall_id IS NOT NULL
),
departure_portcalls AS (
    SELECT
        portcall.id,
        portcall.date_utc,
        portcall.port_id,
        load_status,
        move_type,
        port_operation,
        port.unlocode,
        port.name,
        port.check_departure,
        portcall.ship_imo,
        lead(load_status, -1) OVER (PARTITION BY ship_imo ORDER BY date_utc) AS previous_load_status
FROM
    portcall
    LEFT JOIN port ON portcall.port_id = port.id
        LEFT JOIN ship ON ship.imo = portcall.ship_imo
    WHERE
        date_utc >= '2021-11-01'
        AND ship.commodity != 'unknown'
        AND move_type = 'departure'
    ORDER BY
        date_utc
),
ships_in_ballast AS (
    SELECT DISTINCT
        ship_imo
    FROM
        portcall
    WHERE
        load_status = 'in_ballast'
),
departures_russia AS (
    SELECT
        *,
        lead(date_utc, 1) OVER (PARTITION BY ship_imo ORDER BY date_utc) AS next_russia_departure_date_utc
    FROM
        departure_portcalls
    WHERE
        check_departure
        AND move_type = 'departure'
        AND port_operation = 'load'
),
departures_russia_full AS (
    SELECT
        *
    FROM
        departures_russia
    WHERE
        load_status = 'fully_laden'
),
next_departure AS (
    SELECT DISTINCT ON (departure_portcall_id)
        d.ship_imo AS ship_imo,
        d.port_id AS departure_port_id,
        d.id AS departure_portcall_id,
        d.date_utc AS departure_date_utc,
        d.unlocode AS departure_unlocode,
        d.load_status AS departure_load_status,
        d.load_status AS departure_movetype,
        d.next_russia_departure_date_utc AS next_russia_departure_date_utc,
        nextd.id AS nextdeparture_portcall_id,
        nextd.date_utc AS nextdeparture_date_utc,
        nextd.unlocode AS nextdeparture_unlocode,
        nextd.load_status AS nextdeparture_load_status,
        nextd.move_type AS nextdeparture_move_type,
        nextd.port_operation AS nextdeparture_port_operation
    FROM
        departures_russia_full d
        LEFT JOIN departure_portcalls nextd ON d.ship_imo = nextd.ship_imo
    WHERE
        nextd.move_type = 'departure'
        AND nextd.date_utc > d.date_utc
        AND (d.next_russia_departure_date_utc IS NULL
            OR nextd.date_utc <= d.next_russia_departure_date_utc)
        AND (nextd.port_operation = 'discharge'
            OR (nextd.previous_load_status = 'fully_laden'
                AND nextd.load_status = 'in_ballast')
            -- some boats never seem to reach "in_ballast" or have "discharge"
            -- if a new departure exist from russia afterwards, then we loosen conditions
            OR (d.next_russia_departure_date_utc IS NOT NULL
                AND d.ship_imo NOT IN (
                    SELECT
                        ship_imo
                    FROM
                        ships_in_ballast)
                    AND nextd.previous_load_status = 'fully_laden'
                    AND nextd.load_status = 'partially_laden'))
        ORDER BY
            departure_portcall_id,
            nextd.date_utc
),
-- perform a second join to keep departures_russia_full that don't have yet a next departure
next_departure_full AS (
    SELECT DISTINCT ON (departure_portcall_id)
        d.ship_imo AS ship_imo,
        d.port_id AS departure_port_id,
        d.id AS departure_portcall_id,
        d.date_utc AS departure_date_utc,
        d.unlocode AS departure_unlocode,
        d.load_status AS departure_load_status,
        d.load_status AS departure_movetype,
        d.next_russia_departure_date_utc AS next_russia_departure_date_utc,
        nextdeparture_portcall_id,
        nextdeparture_date_utc,
        nextdeparture_unlocode,
        nextdeparture_load_status,
        nextdeparture_move_type,
        nextdeparture_port_operation
    FROM
        departures_russia_full d
        LEFT JOIN next_departure nd ON d.id = nd.departure_portcall_id
    -- very important not to erase or modify previously completed shipments
    WHERE d.id NOT IN (
            SELECT
                id
            FROM
                completed_departure_portcalls
            WHERE
                id IS NOT NULL)
),
previous_arrival AS (
    SELECT DISTINCT ON (departure_portcall_id,
        nextdeparture_portcall_id)
        nextd.ship_imo AS ship_imo,
        nextd.departure_date_utc,
        nextd.departure_unlocode,
        nextd.departure_port_id,
        nextd.departure_portcall_id,
        preva.id AS arrival_portcall_id,
        preva.date_utc AS arrival_date_utc,
        preva.port_id AS arrival_port_id
    FROM
        next_departure_full nextd
        LEFT JOIN portcall preva --previous arrival
        ON preva.ship_imo = nextd.ship_imo
    WHERE
        preva.date_utc <= nextd.nextdeparture_date_utc
        AND preva.move_type = 'arrival'
        AND preva.date_utc > nextd.departure_date_utc
        AND preva.id NOT IN (
            SELECT
                id
            FROM
                completed_arrival_portcalls
        )
        ORDER BY
            departure_portcall_id,
            nextdeparture_portcall_id,
            preva.date_utc DESC
),
completed_shipments AS (
    SELECT
        *,
        NEXTVAL('departure_id_seq') departure_id,
        NEXTVAL('arrival_id_seq') arrival_id,
    'completed' status
FROM
    previous_arrival
),
uncompleted_shipments AS (
    SELECT
        *,
        NEXTVAL('departure_id_seq') departure_id,
    NULL::bigint arrival_id,
    CASE WHEN nd.next_russia_departure_date_utc IS NULL THEN
        'ongoing'
    ELSE
        'undetected_arrival'
    END AS status
FROM
    next_departure_full nd
    WHERE
        nd.departure_portcall_id NOT IN (
            SELECT
                departure_portcall_id
            FROM
                previous_arrival)
),
shipments AS (
    SELECT
        departure_portcall_id,
        departure_port_id,
        ship_imo,
        departure_date_utc,
        arrival_portcall_id,
        departure_id,
        arrival_id,
        status
    FROM
        completed_shipments
    UNION ALL
    SELECT
        departure_portcall_id,
        departure_port_id,
        ship_imo,
        departure_date_utc,
        NULL::bigint AS arrival_portcall_id,
        departure_id,
        arrival_id,
        status
    FROM
        uncompleted_shipments
),
inserted_departures AS (
INSERT INTO departure (id, port_id, ship_imo, date_utc, method_id, portcall_id)
    SELECT
        departure_id,
        departure_port_id,
        ship_imo,
        departure_date_utc,
        'postgres',
        departure_portcall_id
    FROM
        shipments
    ON CONFLICT (portcall_id)
        DO UPDATE SET
            port_id = excluded.port_id -- just for id to be returned
        RETURNING
            id,
            portcall_id
),
inserted_arrivals AS (
INSERT INTO arrival (id, departure_id, date_utc, method_id, port_id, portcall_id)
    SELECT
        arrival_id,
        inserted_departures.id,
        arrival_date_utc,
        'postgres',
        arrival_port_id,
        arrival_portcall_id
    FROM
        completed_shipments
        LEFT JOIN inserted_departures ON completed_shipments.departure_portcall_id = inserted_departures.portcall_id
     ON CONFLICT (departure_id)
        DO UPDATE SET
            portcall_id = excluded.portcall_id
        RETURNING
            id,
            portcall_id
),
shipments_after_insertion AS (
    SELECT
        departure_portcall_id,
        inserted_departures.id AS departure_id,
        arrival_portcall_id,
        inserted_arrivals.id AS arrival_id,
        status
    FROM
        shipments
        LEFT JOIN inserted_departures ON shipments.departure_portcall_id = inserted_departures.portcall_id
        LEFT JOIN inserted_arrivals ON shipments.arrival_portcall_id = inserted_arrivals.portcall_id
),
inserted_shipments AS (
INSERT INTO shipment (departure_id, arrival_id, status)
    SELECT
        departure_id,
        arrival_id,
        status
    FROM
        shipments_after_insertion
    ON CONFLICT (departure_id)
        DO UPDATE SET
            arrival_id = EXCLUDED.arrival_id,
            status = EXCLUDED.status
        RETURNING
            departure_id,
            arrival_id,
            status
)
    SELECT
        status,
        count(*)
    FROM
        inserted_shipments
    GROUP BY
        1;