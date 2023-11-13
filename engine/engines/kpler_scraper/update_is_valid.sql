--KPLER sometimes changes destination or commodity of recent flows / trades
--This script is here to ensure that we don't double count those that have been updated
--by declaring "invalid" the flows that have been updated since then
-- FLOWS
with last_update as (
    select
        from_zone_id,
        from_split,
        to_split,
        date,
        platform,
        max(updated_on) as updated_on_max
    from
        kpler_flow
    group by
        from_zone_id,
        from_split,
        to_split,
        date,
        platform
)
update
    kpler_flow
set
    is_valid = updated_on_max - updated_on < '2 hours'
from
    last_update
where
    kpler_flow.from_zone_id = last_update.from_zone_id
    and kpler_flow.from_split = last_update.from_split
    and kpler_flow.to_split = last_update.to_split
    and kpler_flow.date = last_update.date
    and kpler_flow.platform = last_update.platform;

-- TRADES
with last_update as (
    select
        id,
        max(updated_on) as updated_on_max
    from
        kpler_trade
    group by
        id
)
update
    kpler_trade
set
    is_valid = is_valid
    AND (updated_on_max - updated_on < '2 hours')
from
    last_update
where
    kpler_trade.id = last_update.id;

-- TRADES (using departure date, zones, flow_id and value_tonne)
BEGIN;

update
    kpler_trade
set
    is_valid = True;

with last_update as (
    select
        flow_id,
        departure_date_utc,
        departure_zone_id,
        arrival_zone_id,
        value_tonne,
        max(updated_on) as updated_on_max
    from
        kpler_trade
    group by
        flow_id,
        departure_date_utc,
        departure_zone_id,
        arrival_zone_id,
        value_tonne
)
update
    kpler_trade
set
    is_valid = (is_valid)
    and (updated_on_max - updated_on < '2 hours')
from
    last_update
where
    kpler_trade.flow_id = last_update.flow_id
    and kpler_trade.departure_date_utc = last_update.departure_date_utc
    and kpler_trade.departure_zone_id = last_update.departure_zone_id
    and kpler_trade.arrival_zone_id = last_update.arrival_zone_id
    and kpler_trade.value_tonne = last_update.value_tonne;

-- version where no arrival_zone_id is indicated
with last_update as (
    select
        flow_id,
        departure_date_utc,
        departure_zone_id,
        vessel_imos,
        value_tonne,
        max(updated_on) as updated_on_max
    from
        kpler_trade
    where
        arrival_zone_id is NULL
    group by
        flow_id,
        departure_date_utc,
        departure_zone_id,
        vessel_imos,
        value_tonne
)
update
    kpler_trade
set
    is_valid = (is_valid)
    and (updated_on_max - updated_on < '2 hours')
from
    last_update
where
    kpler_trade.flow_id = last_update.flow_id
    and kpler_trade.departure_date_utc = last_update.departure_date_utc
    and kpler_trade.departure_zone_id = last_update.departure_zone_id
    and kpler_trade.arrival_zone_id is NULL
    and kpler_trade.value_tonne = last_update.value_tonne;

-- Also allows for change of arrival zone if ongoing
with last_update as (
    select
        flow_id,
        departure_date_utc,
        departure_zone_id,
        vessel_imos,
        value_tonne,
        max(updated_on) as updated_on_max
    from
        kpler_trade
    group by
        flow_id,
        departure_date_utc,
        departure_zone_id,
        vessel_imos,
        value_tonne
)
update
    kpler_trade
set
    is_valid = (is_valid)
    and (updated_on_max - updated_on < '2 hours')
from
    last_update
where
    kpler_trade.flow_id = last_update.flow_id
    and kpler_trade.departure_date_utc = last_update.departure_date_utc
    and kpler_trade.departure_zone_id = last_update.departure_zone_id
    and kpler_trade.vessel_imos = last_update.vessel_imos
    and kpler_trade.value_tonne = last_update.value_tonne
    and kpler_trade.status = 'ongoing';

-- Version where value_tonne is changing
with last_update as (
    select
        flow_id,
        departure_date_utc,
        departure_zone_id,
        vessel_imos,
        arrival_date_utc,
        arrival_zone_id,
        max(updated_on) as updated_on_max
    from
        kpler_trade
    group by
        flow_id,
        departure_date_utc,
        departure_zone_id,
        vessel_imos,
        arrival_date_utc,
        arrival_zone_id
)
update
    kpler_trade
set
    is_valid = (is_valid)
    and (updated_on_max - updated_on < '2 hours')
from
    last_update
where
    kpler_trade.flow_id = last_update.flow_id
    and kpler_trade.departure_date_utc = last_update.departure_date_utc
    and kpler_trade.departure_zone_id = last_update.departure_zone_id
    and kpler_trade.vessel_imos = last_update.vessel_imos
    and kpler_trade.arrival_date_utc = last_update.arrival_date_utc
    and kpler_trade.arrival_zone_id = last_update.arrival_zone_id;

-- Most aggressive ones
with last_update as (
    select
        flow_id,
        departure_date_utc,
        departure_zone_id,
        max(updated_on) as updated_on_max
    from
        kpler_trade
    group by
        flow_id,
        departure_date_utc,
        departure_zone_id
)
update
    kpler_trade
set
    is_valid = (is_valid)
    and (updated_on_max - updated_on < '2 hours')
from
    last_update
where
    kpler_trade.flow_id = last_update.flow_id
    and kpler_trade.departure_date_utc = last_update.departure_date_utc
    and kpler_trade.departure_zone_id = last_update.departure_zone_id;

-- This one is to remove old partial updates, can be quite 'aggressive'
-- Theroretically should fix all of them
WITH last_update AS (
    SELECT
        sub.origin_date,
        sub.country_iso2_departure,
        sub.product_id,
        MAX(sub.updated_on_max) AS updated_on_max
    FROM
        (
            SELECT
                DATE(kt.departure_date_utc) AS origin_date,
                kz.country_iso2 AS country_iso2_departure,
                kt.product_id,
                MAX(kt.updated_on) OVER (
                    PARTITION BY kz.country_iso2,
                    kt.product_id
                    ORDER BY
                        DATE(kt.departure_date_utc),
                        kt.updated_on ROWS BETWEEN UNBOUNDED PRECEDING
                        AND CURRENT ROW
                ) AS updated_on_max
            FROM
                kpler_trade AS kt
                JOIN kpler_zone AS kz ON kt.departure_zone_id = kz.id
        ) AS sub
    GROUP BY
        sub.origin_date,
        sub.country_iso2_departure,
        sub.product_id
)
UPDATE
    kpler_trade AS kt
SET
    is_valid = (kt.is_valid)
    AND (
        lu.updated_on_max - kt.updated_on < interval '7 days'
    )
FROM
    last_update AS lu,
    kpler_zone AS kz
WHERE
    kt.departure_zone_id = kz.id
    AND kz.country_iso2 = lu.country_iso2_departure
    AND kt.product_id = lu.product_id
    AND DATE(kt.departure_date_utc) = lu.origin_date;
