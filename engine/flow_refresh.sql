with completed_portcalls as (
    select departure.portcall_id as id
    from flow
    left join departure on flow.departure_id = departure.id
    where flow.status='completed'
),

departure_portcalls_all as (
	select portcall.id, portcall.date_utc, portcall.port_id, load_status, move_type, port_operation,
	port.unlocode, port.name, port.check_departure, portcall.ship_imo,

 	lead(load_status, -1)
 	OVER (
     	PARTITION BY ship_imo
 	    ORDER BY date_utc
 	) as previous_load_status
	from portcall
	left join port on portcall.port_id=port.id
	left join ship on ship.imo = portcall.ship_imo
	where date_utc >= '2022-01-01'
	and ship.commodity != 'unknown'
--  	and ship_imo='9327372'
	and move_type='departure'
--   	where ship_imo in (select imo from ship limit 100)
	order by date_utc
),


departure_portcalls as (
    select * from departure_portcalls_all
    where id not in (select id from completed_portcalls)
),


ships_in_ballast as (
    select distinct ship_imo
    from portcall
    where load_status='in_ballast'
),

departures_russia as (
	select *,
	lead(date_utc, 1)
	OVER (
    	PARTITION BY ship_imo
	    ORDER BY date_utc
	) as next_russia_departure_date_utc
	from departure_portcalls
	where check_departure and move_type='departure' and port_operation='load'
),

departures_russia_full as (
	select * from departures_russia
	where load_status = 'fully_laden'
),


next_departure as (
	select distinct on (departure_portcall_id)
		d.ship_imo as ship_imo,
		d.port_id as departure_port_id,
		d.id as departure_portcall_id,
		d.date_utc as departure_date_utc,
		d.unlocode as departure_unlocode,
		d.load_status as departure_load_status,
		d.load_status as departure_movetype,
		d.next_russia_departure_date_utc as next_russia_departure_date_utc,
	    nextd.id as nextdeparture_portcall_id,
		nextd.date_utc as nextdeparture_date_utc,
 		nextd.unlocode as nextdeparture_unlocode,
		nextd.load_status as nextdeparture_load_status,
		nextd.move_type as nextdeparture_move_type,
		nextd.port_operation as nextdeparture_port_operation
	from departures_russia_full d
	left join departure_portcalls nextd on
		d.ship_imo=nextd.ship_imo
	WHERE
		nextd.move_type='departure'
		AND nextd.date_utc > d.date_utc
		AND (d.next_russia_departure_date_utc is null or nextd.date_utc <= d.next_russia_departure_date_utc)
 		AND (nextd.port_operation='discharge'
 		        OR (nextd.previous_load_status='fully_laden' AND nextd.load_status='in_ballast')

 		        -- some boats never seem to reach "in_ballast" or have "discharge"
 		        -- if a new departure exist from russia afterwards, then we loosen conditions
 		        OR (d.next_russia_departure_date_utc is not null
 		            AND d.ship_imo not in (select ship_imo from ships_in_ballast)
 		            AND nextd.previous_load_status='fully_laden'
 		            AND nextd.load_status='partially_laden')
 		        )
	order by departure_portcall_id, nextd.date_utc
),

-- perform a second join to keep departures_russia_full that don't have yet a next departure
next_departure_full as (
select distinct on (departure_portcall_id)
		d.ship_imo as ship_imo,
		d.port_id as departure_port_id,
		d.id as departure_portcall_id,
		d.date_utc as departure_date_utc,
		d.unlocode as departure_unlocode,
		d.load_status as departure_load_status,
		d.load_status as departure_movetype,
	    d.next_russia_departure_date_utc as next_russia_departure_date_utc,
	    nextdeparture_portcall_id,
		nextdeparture_date_utc,
 		nextdeparture_unlocode,
		nextdeparture_load_status,
		nextdeparture_move_type,
		nextdeparture_port_operation
	from departures_russia_full d
	left join next_departure nd
	on d.id=nd.departure_portcall_id
),


previous_arrival as (
	select distinct on (departure_portcall_id, nextdeparture_portcall_id)
		nextd.ship_imo as ship_imo,
		nextd.departure_date_utc,
		nextd.departure_unlocode,
	 	nextd.departure_port_id,
		nextd.departure_portcall_id,
		preva.id as arrival_portcall_id,
		preva.date_utc as arrival_date_utc,
		preva.port_id as arrival_port_id
	from next_departure_full nextd
	left join portcall preva --previous arrival
	on preva.ship_imo=nextd.ship_imo
	where preva.date_utc < nextd.nextdeparture_date_utc
	and preva.move_type='arrival'
	and preva.date_utc > nextd.departure_date_utc
	order by departure_portcall_id, nextdeparture_portcall_id, preva.date_utc desc
),


completed_flows as (
	select *, NEXTVAL('departure_id_seq') departure_id, NEXTVAL('arrival_id_seq') arrival_id, 'completed' status
	from previous_arrival
	where departure_portcall_id not in (select id from completed_portcalls)
),

uncompleted_flows as (
	select *, NEXTVAL('departure_id_seq') departure_id, NULL::bigint arrival_id,
	CASE WHEN nd.next_russia_departure_date_utc is NULL THEN 'ongoing'
	ELSE 'arrival_undetected' END as status
	from next_departure_full nd
	where nd.departure_portcall_id not in (select departure_portcall_id from previous_arrival)
	and departure_portcall_id not in (select id from completed_portcalls)
),

flows as (
    select departure_portcall_id, departure_port_id, ship_imo, departure_date_utc,
	arrival_portcall_id, departure_id, arrival_id, status from completed_flows
    union all
    select departure_portcall_id, departure_port_id, ship_imo, departure_date_utc,
	NULL::bigint as arrival_portcall_id, departure_id, arrival_id, status from uncompleted_flows
),

   inserted_departures as (
   	INSERT INTO departure (id, port_id, ship_imo, date_utc, method_id, portcall_id)
   	SELECT departure_id, departure_port_id, ship_imo, departure_date_utc, 'postgres', departure_portcall_id
   	from flows
	   ON CONFLICT (portcall_id) DO UPDATE SET port_id=excluded.port_id -- just for id to be returned
	returning id, portcall_id
   ),

  inserted_arrivals as (
  	INSERT INTO arrival (id, departure_id, date_utc, method_id, port_id, portcall_id)
  	SELECT arrival_id, departure_id, arrival_date_utc, 'postgres', arrival_port_id, arrival_portcall_id
  	from completed_flows
	ON CONFLICT (portcall_id) DO UPDATE SET port_id=excluded.port_id -- just for id to be returned
	  RETURNING id, portcall_id
  ),

  flows_after_insertion as (
 	select
	  departure_portcall_id, inserted_departures.id as departure_id,
	  arrival_portcall_id, inserted_arrivals.id as arrival_id,
	  status
	  from flows
     left join inserted_departures on flows.departure_portcall_id=inserted_departures.portcall_id
 	left join inserted_arrivals on 	 flows.arrival_portcall_id=inserted_arrivals.portcall_id
 ),

   inserted_flows as (
   	INSERT INTO flow (departure_id, arrival_id, status)
   	SELECT departure_id, arrival_id, status
   	from flows_after_insertion
 	 ON CONFLICT (departure_id) DO UPDATE
 	SET arrival_id = EXCLUDED.arrival_id,
 	  status = EXCLUDED.status
	returning departure_id, arrival_id, status
   )

 select status, count(*) from inserted_flows group by 1;
