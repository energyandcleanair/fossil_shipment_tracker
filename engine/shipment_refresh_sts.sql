-- first delete existing sts shipments for refresh - we do this as we have non-unqiue departure/arrival ids and hence
-- we refresh whole table each time
WITH deleted_sts_shipments AS (
     delete from shipment_with_sts
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
-- take unique events by collapsing all of them between discharge calls and taking distinct ship name/int ship name
unique_events AS (
    SELECT
  DISTINCT ON (
    next_departure_events.ship_imo,
    next_departure_events.next_departure_date_utc,
    next_departure_events.interacting_ship_name
  )
    next_departure_events.*
FROM
  (
    SELECT
      dr.ship_imo,
      dr.date_utc,
      next_departures.next_departure_date_utc,
      ev.id AS event_id,
      ev.ship_name,
      ev.interacting_ship_name,
      ev.date_utc AS event_date_utc,
      ev.interacting_ship_imo,
      cast(
        ev.interacting_ship_details ->> 'distance_meters' AS bigint
      ) AS distance_meters
    FROM departures_russia_full dr
    LEFT JOIN
      (
        SELECT DISTINCT ON (d.ship_imo, d.id)
          d.ship_imo,
          d.id as departure_portcall_id,
          d.date_utc,
          p.date_utc AS next_departure_date_utc
        FROM
          departures_russia_full d
        LEFT JOIN departure_portcalls p
            ON p.ship_imo = d.ship_imo
        WHERE
            p.move_type = 'departure'
            AND p.date_utc > d.date_utc
            AND (d.next_russia_departure_date_utc IS NULL
                OR p.date_utc <= d.next_russia_departure_date_utc)
            AND (p.port_operation = 'discharge'
                OR (p.previous_load_status = 'fully_laden'
                    AND p.load_status = 'in_ballast')
                OR p.port_operation = 'load')
        ORDER BY
            d.ship_imo,
            d.id,
            next_departure_date_utc DESC
      ) AS next_departures
      ON dr.id = next_departures.departure_portcall_id
      LEFT JOIN event ev ON (
        ev.ship_imo = dr.ship_imo
        AND (
          (
            next_departures.next_departure_date_utc IS NOT NULL
            AND ev.date_utc BETWEEN dr.date_utc
            AND next_departures.next_departure_date_utc
          )
          OR (
            next_departures.next_departure_date_utc IS NULL
            AND ev.date_utc BETWEEN dr.date_utc
            AND CURRENT_DATE
          )
        )
      )
    WHERE
      ev.type_id = '21'
      AND ev.interacting_ship_imo IS NOT NULL
  ) AS next_departure_events
ORDER BY
  next_departure_events.ship_imo,
  next_departure_events.next_departure_date_utc,
  next_departure_events.interacting_ship_name,
  next_departure_events.event_date_utc DESC
),
departures_sts AS (
    SELECT
        e.interacting_ship_imo AS ship_imo,
        e.event_id,
        e.event_date_utc as departure_date_utc
    FROM
        unique_events e
    WHERE
        e.interacting_ship_imo IS NOT NULL
        AND e.event_id IS NOT NULL
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
),
sts_arrival AS (
    SELECT DISTINCT ON (departure_portcall_id,
        event_id)
        nextd.ship_imo AS ship_imo,
        nextd.departure_date_utc,
        nextd.departure_unlocode,
        nextd.departure_port_id,
        nextd.departure_portcall_id,
        ev.event_id,
        ev.event_date_utc
    FROM
        next_departure_full nextd
        LEFT JOIN unique_events ev
            ON ev.ship_imo = nextd.ship_imo
    WHERE
        ev.event_date_utc > nextd.departure_date_utc AND
        (
            (nextd.next_russia_departure_date_utc IS NOT NULL AND ev.event_date_utc <= nextd.next_russia_departure_date_utc)
            OR
            (nextd.next_russia_departure_date_utc IS NULL AND ev.event_date_utc <= CURRENT_DATE)
        )
        ORDER BY
            departure_portcall_id,
            event_id,
            ev.event_date_utc DESC
),
-- now look at departures from sts and check if we have any matching arrivals
sts_departures_with_next AS (
    SELECT DISTINCT ON (departure_event_id)
        d.ship_imo,
        d.departure_date_utc,
        d.event_id as departure_event_id,
        nextd.id AS nextdeparture_portcall_id,
        nextd.date_utc AS nextdeparture_date_utc,
        nextd.unlocode AS nextdeparture_unlocode,
        nextd.load_status AS nextdeparture_load_status,
        nextd.move_type AS nextdeparture_move_type,
        nextd.port_operation AS nextdeparture_port_operation
    FROM
        departures_sts d
    LEFT JOIN departure_portcalls nextd
            ON d.ship_imo = nextd.ship_imo
    WHERE
        -- find next departure
        nextd.move_type = 'departure'
        AND nextd.date_utc > d.departure_date_utc
        AND (nextd.port_operation = 'discharge'
            OR (nextd.previous_load_status = 'fully_laden'
                AND nextd.load_status = 'in_ballast'))
    ORDER BY
        d.event_id,
        nextd.id,
        d.departure_date_utc
),
-- some sts departures wont have a next departure, so we merge to keep full
sts_departures_with_next_full AS (
    SELECT DISTINCT ON (ship_imo, departure_event_id)
        d.ship_imo,
        d.departure_date_utc,
        d.event_id AS departure_event_id,
        nextdsts.nextdeparture_portcall_id,
        nextdsts.nextdeparture_date_utc,
        nextdsts.nextdeparture_unlocode,
        nextdsts.nextdeparture_load_status,
        nextdsts.nextdeparture_move_type,
        nextdsts.nextdeparture_port_operation
    FROM
        departures_sts d
        LEFT JOIN sts_departures_with_next nextdsts
            ON d.event_id = nextdsts.departure_event_id
    ORDER BY
        ship_imo,
        departure_event_id,
        d.departure_date_utc ASC
),
sts_departures_with_arrival AS (
    SELECT DISTINCT ON (departure_event_id)
        nextd.departure_event_id,
        nextd.ship_imo AS ship_imo,
        nextd.departure_date_utc,
        preva.id AS arrival_portcall_id,
        preva.date_utc AS arrival_date_utc,
        preva.port_id AS arrival_port_id
    FROM
        sts_departures_with_next_full nextd
        LEFT JOIN portcall preva --previous arrival
            ON preva.ship_imo = nextd.ship_imo
    WHERE
        preva.date_utc <= nextd.nextdeparture_date_utc
        AND preva.move_type = 'arrival'
        AND preva.date_utc > nextd.departure_date_utc
        ORDER BY
            departure_event_id,
            arrival_portcall_id,
            preva.date_utc DESC
),
completed_shipments_with_sts_arrival AS (
    SELECT
        departure_portcall_id,
        departure_port_id,
        ship_imo,
        departure_date_utc,
        NULL::bigint AS arrival_portcall_id,
        NULL::bigint AS arrival_port_id,
        event_date_utc as arrival_date_utc,
        NEXTVAL('arrival_id_seq') arrival_id,
        'completed' status,
        event_id AS arrival_event_id,
        NULL::bigint AS departure_event_id
    FROM
        sts_arrival
    WHERE
        event_id IS NOT NULL
),
completed_shipments_with_sts_departure AS (
    SELECT
        NULL::bigint AS departure_portcall_id,
        NULL::bigint AS departure_port_id,
        ship_imo,
        departure_date_utc,
        arrival_portcall_id,
        arrival_port_id,
        arrival_date_utc,
        NEXTVAL('arrival_id_seq') arrival_id,
        'completed' status,
        departure_event_id,
        NULL::bigint AS arrival_event_id
    FROM
        sts_departures_with_arrival
),
uncompleted_shipments_with_sts_departure AS (
    SELECT
        nd.departure_event_id,
        NULL::bigint AS arrival_event_id,
        nd.departure_date_utc,
        nd.ship_imo,
        NULL::bigint arrival_id,
        NULL::bigint arrival_port_id,
        NULL::bigint arrival_portcall_id,
        NULL::bigint arrival_date_utc,
        NULL::bigint departure_port_id,
        NULL::bigint departure_portcall_id,
        'ongoing' AS status
    FROM
        sts_departures_with_next_full nd
    WHERE
        nd.departure_event_id NOT IN (
            SELECT
                departure_event_id
            FROM
                completed_shipments_with_sts_departure
                )
        AND nd.departure_event_id IS NOT NULL
),
shipments_sts AS (
    SELECT
        departure_portcall_id,
        departure_port_id,
        ship_imo,
        departure_date_utc,
        arrival_portcall_id,
        arrival_id,
        status,
        arrival_event_id,
        departure_event_id
    FROM
        completed_shipments_with_sts_arrival
    UNION ALL
    SELECT
        departure_portcall_id,
        departure_port_id,
        ship_imo,
        departure_date_utc,
        arrival_portcall_id,
        arrival_id,
        status,
        arrival_event_id,
        departure_event_id
    FROM
        completed_shipments_with_sts_departure
    UNION ALL
    SELECT
        departure_portcall_id,
        departure_port_id,
        ship_imo,
        departure_date_utc,
        arrival_portcall_id,
        arrival_id,
        status,
        arrival_event_id,
        departure_event_id
    FROM
        uncompleted_shipments_with_sts_departure
),
sts_departures AS (
    SELECT DISTINCT ON (departure_portcall_id, departure_event_id, departure_port_id, ship_imo)
        NEXTVAL('departure_id_seq') departure_id,
        departure_port_id,
        ship_imo,
        departure_date_utc,
        'postgres',
        departure_portcall_id,
        departure_event_id
    FROM
        shipments_sts
    ORDER BY
        departure_portcall_id,
        departure_event_id,
        departure_port_id,
        ship_imo DESC
),
inserted_departures AS (
INSERT INTO departure (id, port_id, ship_imo, date_utc, method_id, portcall_id, event_id)
    SELECT
        departure_id,
        departure_port_id,
        ship_imo,
        departure_date_utc,
        'postgres',
        departure_portcall_id,
        departure_event_id
    FROM
        sts_departures
    ON CONFLICT (portcall_id)
        DO UPDATE SET
            port_id = excluded.port_id -- just for id to be returned
        RETURNING
            id,
            portcall_id,
            event_id
),
completed_shipments_all AS (
    SELECT
        arrival_id,
        arrival_date_utc,
        'postgres',
        arrival_port_id,
        arrival_portcall_id,
        departure_event_id,
        arrival_event_id,
        departure_portcall_id
    FROM
        completed_shipments_with_sts_arrival
    UNION ALL
    SELECT
        arrival_id,
        arrival_date_utc,
        'postgres',
        arrival_port_id,
        arrival_portcall_id,
        departure_event_id,
        arrival_event_id,
        departure_portcall_id
    FROM
        completed_shipments_with_sts_departure
),
inserted_arrivals AS (
INSERT INTO arrival (id, departure_id, date_utc, method_id, port_id, portcall_id, event_id)
    SELECT
        arrival_id,
        inserted_departures.id,
        arrival_date_utc,
        'postgres',
        arrival_port_id,
        arrival_portcall_id,
        arrival_event_id
    FROM
        completed_shipments_all
        LEFT JOIN inserted_departures ON
        (
            (completed_shipments_all.departure_portcall_id IS NOT NULL AND completed_shipments_all.departure_portcall_id = inserted_departures.portcall_id)
            OR
            (completed_shipments_all.departure_portcall_id IS NULL AND completed_shipments_all.departure_event_id = inserted_departures.event_id)
        )
     ON CONFLICT
        DO NOTHING
        RETURNING
            id,
            portcall_id,
            event_id
),
shipments_after_insertion AS (
    SELECT
        inserted_departures.id AS departure_id,
        completed_shipments_all.arrival_id AS arrival_id,
        status
    FROM
        shipments_sts
        LEFT JOIN inserted_departures ON
            (
                (shipments_sts.departure_portcall_id IS NOT NULL AND shipments_sts.departure_portcall_id = inserted_departures.portcall_id)
                OR
                (shipments_sts.departure_portcall_id IS NULL AND shipments_sts.departure_event_id = inserted_departures.event_id)
            )
        LEFT JOIN completed_shipments_all ON
            (
                (shipments_sts.arrival_portcall_id IS NOT NULL AND shipments_sts.departure_event_id IS NOT NULL
                    AND shipments_sts.arrival_portcall_id = completed_shipments_all.arrival_portcall_id AND shipments_sts.departure_event_id = completed_shipments_all.departure_event_id)
                OR
                (shipments_sts.arrival_portcall_id IS NOT NULL AND shipments_sts.departure_event_id IS NULL
                    AND shipments_sts.arrival_portcall_id = completed_shipments_all.arrival_portcall_id)
                OR
                (shipments_sts.arrival_portcall_id IS NULL AND shipments_sts.arrival_event_id = completed_shipments_all.arrival_event_id)
            )
),
inserted_shipments AS (
INSERT INTO shipment_with_sts (id, departure_id, arrival_id, status)
    SELECT
        NEXTVAL('shipment_id_seq') id,
        departure_id,
        arrival_id,
        status
    FROM
        shipments_after_insertion
    ON CONFLICT
        DO NOTHING
        RETURNING
            departure_id,
            arrival_id,
            status
),
--- delete any shipments that were in non sts shipment table that now have a sts arrival (for example ongoing shipments)
deleted_shipments AS (
    DELETE FROM shipment
        WHERE departure_id IN (
            SELECT
                departure_id
            FROM
                inserted_shipments
        )
)
    SELECT
        status,
        count(*)
    FROM
        inserted_shipments
    GROUP BY
        1;