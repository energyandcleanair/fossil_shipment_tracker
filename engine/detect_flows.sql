delete from flowarrivalberth;
delete from position;
delete from flow;
delete from arrival;
delete from departure;


with portcalls as (
	select portcall.id, portcall.date_utc, portcall.port_id, load_status, move_type, port_operation,
	port.unlocode, port.name, port.check_departure, portcall.ship_imo,

 	lead(load_status, -1)
 	OVER (
     	PARTITION BY ship_imo
 	    ORDER BY date_utc
 	) as previous_load_status
	from portcall left join port on portcall.port_id=port.id
-- 	where ship_imo='9794446'
--   	where ship_imo in (select imo from ship limit 100)
	order by date_utc
),
departures_russia as (
	select *,
	lead(date_utc, 1)
	OVER (
    	PARTITION BY ship_imo
	    ORDER BY date_utc
	) as next_russia_departure_date_utc
	from portcalls
	where check_departure and move_type='departure'
),
departures_russia_full as (
	select * from departures_russia
	where load_status = 'fully_laden'
),
-- insert into departure
next_departure as (
	select distinct on (departure_portcall_id)
		d.ship_imo as ship_imo,
		d.port_id as departure_port_id,
		d.id as departure_portcall_id,
		d.date_utc as departure_date_utc,
		d.unlocode as departure_unlocode,
		d.load_status as departure_load_status,
		d.load_status as departure_movetype,
	    nextd.id as nextdeparture_portcall_id,
		nextd.date_utc as nextdeparture_date_utc,
 		nextd.unlocode as nextdeparture_unlocode,
		nextd.load_status as nextdeparture_load_status,
		nextd.move_type as nextdeparture_move_type
	from departures_russia_full d
	left join portcalls nextd on
		d.ship_imo=nextd.ship_imo
	WHERE
		nextd.move_type='departure'
		AND nextd.date_utc > d.date_utc
		AND (d.next_russia_departure_date_utc is null or nextd.date_utc <= d.next_russia_departure_date_utc)
 		AND (nextd.port_operation='discharge' OR (nextd.previous_load_status='fully_laden' AND nextd.load_status='in_ballast'))
	order by departure_portcall_id, nextd.date_utc


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
	from next_departure nextd
	left join portcalls preva --previous arrival
	on preva.ship_imo=nextd.ship_imo
	where preva.date_utc < nextd.nextdeparture_date_utc and preva.date_utc > nextd.departure_date_utc
	order by departure_portcall_id, nextdeparture_portcall_id, preva.id, preva.date_utc desc
),

flows as (
	select *, NEXTVAL('departure_id_seq') departure_id, NEXTVAL('arrival_id_seq') arrival_id
	from previous_arrival
),

inserted_departures as (
	INSERT INTO departure (id, port_id, ship_imo, date_utc, method_id, portcall_id)
	SELECT departure_id, departure_port_id, ship_imo, departure_date_utc, 'postgres', departure_portcall_id
	from flows
),

inserted_arrivals as (
	INSERT INTO arrival (id, departure_id, date_utc, method_id, port_id, portcall_id)
	SELECT arrival_id, departure_id, arrival_date_utc, 'postgres', arrival_port_id, arrival_portcall_id
	from flows
),

inserted_flows as (
	INSERT INTO flow (departure_id, arrival_id)
	SELECT departure_id, arrival_id
	from flows
)

select * from flows;
