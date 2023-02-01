-- first delete existing sts shipments for refresh - we do this as we have non-unqiue departure/arrival ids and hence
-- we refresh whole table each time
-- CLEAN UP BEGIN
WITH RECURSIVE deleted_sts_shipments AS (
     DELETE FROM shipment_with_sts
     RETURNING
        id,
        departure_id,
        arrival_id
),
deleted_departure_sts AS (
    DELETE FROM departure
    where id IN (
        SELECT
            departure_id
        FROM
            deleted_sts_shipments
    )
),
deleted_arrival_sts AS (
    DELETE FROM arrival
    where id IN (
        SELECT
            arrival_id
        FROM
            deleted_sts_shipments
        WHERE
            arrival_id IS NOT NULL
    )
),
deleted_shipmentarrivalberth_sts AS (
    DELETE FROM shipmentarrivalberth
    where shipment_id IN (
        SELECT
            id
        FROM
            deleted_sts_shipments
    )
),
deleted_shipmentdepartureberth_sts AS (
    DELETE FROM shipmentdepartureberth
    where shipment_id IN (
        SELECT
            id
        FROM
            deleted_sts_shipments
    )
),
deleted_trajectory_sts AS (
    DELETE FROM trajectory
    where shipment_id IN (
        SELECT
            id
        FROM
            deleted_sts_shipments
    )
),

ship_draught AS (
    SELECT
        ship_imo,
        -- min(draught) can yield very low values.
        max(draught * (load_status='in_ballast')::integer) as draught_min,
        max(draught) as draught_max
    FROM portcall
    GROUP BY 1
),

portcall_w_prev AS (
    SELECT *,
    lead(portcall.load_status, 1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS next_load_status,
    lead(portcall.draught, 1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS next_draught,
    lead(portcall.date_utc, 1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS next_date_utc,
    lead(portcall.load_status, -1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS previous_load_status,
    lead(portcall.move_type, -1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS previous_move_type,
    lead(portcall.move_type, 1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS next_move_type,
    lead(portcall.date_utc, -1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS previous_date_utc,
    lead(portcall.port_id, -1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS previous_port_id,
    lead(portcall.id, -1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS previous_portcall_id,
    lead(portcall.draught, -1) OVER (PARTITION BY portcall.ship_imo ORDER BY portcall.date_utc) AS previous_draught
    FROM portcall
    WHERE date_utc >= '2021-01-01'
),

-- CLEAN UP END
departure_portcalls AS (
    SELECT
        pc.id,
        pc.date_utc,
        pc.port_id,
        pc.load_status,
        pc.next_load_status,
        pc.draught,
        pc.next_draught,
        pc.move_type,
        pc.next_move_type,
        pc.port_operation,
        port.unlocode,
        port.name,
        port.check_departure,
        pc.ship_imo,
        pc.draught,
        ship_draught.draught_min,
        ship_draught.draught_max,

        pc.previous_load_status,
        pc.previous_move_type,
        pc.previous_portcall_id,
        pc.previous_date_utc,
        pc.previous_port_id,
        pc.previous_draught,

        -- We need both previous arrival status AND previous departure status
        lead(pc.load_status, -1) OVER (PARTITION BY pc.ship_imo ORDER BY pc.date_utc) AS previous_departure_load_status
FROM
    portcall_w_prev pc
    LEFT JOIN port ON pc.port_id = port.id
    LEFT JOIN ship ON ship.imo = pc.ship_imo
    LEFT JOIN ship_draught ON ship_draught.ship_imo = pc.ship_imo
    WHERE
        pc.date_utc >= '2021-01-01'
        AND ship.commodity != 'unknown'
        AND move_type = 'departure'
    ORDER BY
        pc.date_utc
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
    events.ship_imo,
    events.next_portcall_date_utc,
    events.interacting_ship_imo
  )
    events.*
FROM
  (
  	SELECT
	  ev.ship_imo,
	  ev.date_utc as event_date_utc,
	  ev.id as event_id,
	  ev.interacting_ship_imo,
	  ev.interacting_ship_name,
	  pprev_ship.next_date_utc as next_portcall_date_utc,
	  pprev_intship.next_date_utc as intship_next_portcall_date_utc
	  FROM
	    event ev
	  JOIN ship mainship ON mainship.imo = ev.ship_imo
      JOIN ship intship ON intship.imo = ev.interacting_ship_imo
	  LEFT JOIN portcall_w_prev pprev_ship ON
        (pprev_ship.ship_imo = ev.ship_imo AND ev.date_utc BETWEEN pprev_ship.date_utc AND pprev_ship.next_date_utc)
      LEFT JOIN portcall_w_prev pprev_intship ON
        (pprev_intship.ship_imo = ev.interacting_ship_imo AND ev.date_utc BETWEEN pprev_intship.date_utc AND pprev_intship.next_date_utc)
    WHERE
      ev.type_id = '21'
      AND ev.interacting_ship_imo IS NOT NULL
      AND ev.interacting_ship_details ->> 'distance_meters' IS NOT NULL
      AND (
        (mainship.commodity = intship.commodity) OR
        (mainship.commodity IN ('oil_products', 'oil_or_chemical') AND intship.commodity IN ('oil_products', 'oil_or_chemical'))
      )
      AND pprev_ship.draught > pprev_ship.next_draught
      AND pprev_intship.draught < pprev_intship.next_draught
    ) AS events
    ORDER BY
  events.ship_imo,
  events.next_portcall_date_utc,
  events.interacting_ship_imo,
  events.event_date_utc DESC
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
            OR (nextd.previous_departure_load_status = 'fully_laden'
                AND nextd.load_status = 'in_ballast')
            -- some boats never seem to reach "in_ballast" or have "discharge"
            -- if a new departure exist from russia afterwards, then we loosen conditions
            OR (d.next_russia_departure_date_utc IS NOT NULL
                AND d.ship_imo NOT IN (
                    SELECT
                        ship_imo
                    FROM
                        ships_in_ballast)
                    AND nextd.previous_departure_load_status = 'fully_laden'
                    AND nextd.load_status = 'partially_laden')
             -- When a ship is discharging and loading at the same port (e.g. UST-Luga ANCH)
            OR (nextd.port_operation = 'load'
                AND (
                    nextd.previous_load_status = 'in_ballast' -- this one is an arrival
                    OR (nextd.previous_load_status = 'partially_laden'
                        AND nextd.previous_draught <= nextd.draught_min + 0.1 * (nextd.draught_max - nextd.draught_min)))
                AND nextd.load_status = 'fully_laden'
            ))
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
        (
            (nextd.next_russia_departure_date_utc IS NOT NULL AND ev.event_date_utc BETWEEN nextd.departure_date_utc AND nextd.next_russia_departure_date_utc)
            OR
            (nextd.next_russia_departure_date_utc IS NULL AND ev.event_date_utc BETWEEN nextd.departure_date_utc AND CURRENT_DATE)
        )
        ORDER BY
            departure_portcall_id,
            event_id,
            ev.event_date_utc DESC
),
departures_sts AS (
    SELECT
        e.interacting_ship_imo AS ship_imo,
        e.event_id,
        e.event_date_utc as departure_date_utc
    FROM
        unique_events e
    WHERE
        e.event_id IN (
			SELECT event_id
			FROM sts_arrival
		)
),
-- recursively find the event chain from the sts departures,
event_chain as (
	SELECT
	    ship_imo,
	    interacting_ship_imo,
		event_id as origin_event,
	    null::bigint as event_from,
	    event_id as event_to,
	    event_date_utc,
	    intship_next_portcall_date_utc,
	    1 as level,
	FROM unique_events
		UNION ALL
	SELECT
	    unique_events.ship_imo,
	    unique_events.interacting_ship_imo,
		event_chain.origin_event,
	    event_chain.event_to as event_from,
		unique_events.event_id AS event_to,
	    unique_events.event_date_utc,
	    unique_events.intship_next_portcall_date_utc,
	    event_chain.level + 1
	FROM event_chain
	JOIN unique_events ON (
		unique_events.ship_imo = event_chain.interacting_ship_imo AND
        (unique_events.event_date_utc > event_chain.event_date_utc AND
         unique_events.event_date_utc < event_chain.intship_next_portcall_date_utc)
  )
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
        nextd.nextdeparture_portcall_id,
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
        event_date_utc AS arrival_date_utc,
        NULL::bigint AS nextdeparture_portcall_id,
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
        nextdeparture_portcall_id,
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
        NULL::bigint nextdeparture_portcall_id,
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
        nextdeparture_portcall_id,
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
        nextdeparture_portcall_id,
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
        nextdeparture_portcall_id,
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
        departure_portcall_id,
        nextdeparture_portcall_id
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
        departure_portcall_id,
        nextdeparture_portcall_id
    FROM
        completed_shipments_with_sts_departure
),
inserted_arrivals AS (
INSERT INTO arrival (id, departure_id, date_utc, method_id, port_id, portcall_id, event_id, nextdeparture_portcall_id)
    SELECT
        arrival_id,
        inserted_departures.id,
        arrival_date_utc,
        'postgres',
        arrival_port_id,
        arrival_portcall_id,
        arrival_event_id,
        nextdeparture_portcall_id
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
        NEXTVAL('flow_id_seq') id,
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
--- CLEAN UP BEGIN
deleted_shipments AS (
    DELETE FROM shipment s
        USING departure d
            WHERE d.id = s.departure_id
            AND d.portcall_id IS NOT NULL
            AND d.portcall_id IN (
                SELECT
                    ds.portcall_id
                FROM
                    inserted_shipments ins
                JOIN
                    inserted_departures ds ON ds.id = ins.departure_id
                WHERE
                    ds.portcall_id IS NOT NULL
            )
            RETURNING
                s.id,
                s.departure_id,
                s.arrival_id
),
--- we could do this combined above with USING and OUTER JOIN but it's hacky and more tricky to modify in the future
deleted_shipments_post_sts AS (
    DELETE FROM shipment s
        USING arrival a
            WHERE a.id = s.arrival_id
            AND a.portcall_id IS NOT NULL
            AND a.portcall_id IN (
                SELECT
                    ars.portcall_id
                FROM
                    inserted_shipments ins
                JOIN
                    inserted_arrivals ars ON ars.id = ins.arrival_id
                WHERE
                    ars.portcall_id IS NOT NULL
            )
            RETURNING
                s.id,
                s.departure_id,
                s.arrival_id
),
deleted_trajectory AS (
    DELETE FROM trajectory
        WHERE shipment_id IN (
            SELECT
                id
            FROM
                deleted_shipments
            UNION ALL SELECT
                id
            FROM
                deleted_shipments_post_sts
        )
),
deleted_shipmentdepartureberth AS (
    DELETE FROM shipmentdepartureberth
        WHERE shipment_id IN (
            SELECT
                id
            FROM
                deleted_shipments
            UNION ALL SELECT
                id
            FROM
                deleted_shipments_post_sts
        )
),
deleted_shipmentarrivalberth AS (
    DELETE FROM shipmentarrivalberth
        WHERE shipment_id IN (
            SELECT
                id
            FROM
                deleted_shipments
            UNION ALL SELECT
                id
            FROM
                deleted_shipments_post_sts
        )
),
deleted_departure AS (
    DELETE FROM departure
        WHERE id IN (
            SELECT
                departure_id
            FROM
                deleted_shipments
            UNION ALL SELECT
                departure_id
            FROM
                deleted_shipments_post_sts
        )
),
deleted_arrivals AS (
    DELETE FROM arrival
        WHERE id IN (
            SELECT
                arrival_id
            FROM
                deleted_shipments
            UNION ALL SELECT
                arrival_id
            FROM
                deleted_shipments_post_sts
        )
)
--- CLEAN UP END
    SELECT
        status,
        count(*)
    FROM
        inserted_shipments
    GROUP BY
        1;