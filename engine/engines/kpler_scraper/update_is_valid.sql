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


create or replace view invalid_trade as

with matching_id as (
    with last_update as (
        select
            id,
            max(updated_on) as updated_on_max
        from
            kpler_trade
        group by
            id
    )
    select
        kpler_trade.id as trade_id,
        kpler_trade.flow_id,
        kpler_trade.product_id,
        'matching_id (updates to ID after this)' as reason
    from
        kpler_trade
        join last_update
            on kpler_trade.id = last_update.id
    where
        updated_on_max - updated_on >= '2 hours'
),

matching__flow$dep$arr_zone$value as (
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
    select
        kpler_trade.id as trade_id,
        kpler_trade.flow_id,
        kpler_trade.product_id,
        'matching__flow$dep$arr_zone$value' as reason
    from
        kpler_trade
        join last_update
            on kpler_trade.flow_id = last_update.flow_id
                and kpler_trade.departure_date_utc = last_update.departure_date_utc
                and kpler_trade.departure_zone_id = last_update.departure_zone_id
                and kpler_trade.arrival_zone_id = last_update.arrival_zone_id
                and kpler_trade.value_tonne = last_update.value_tonne
    where
        (updated_on_max - updated_on >= '2 hours')
),

matching_ongoing__flow$dep$vessels$value as (
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
    select
        kpler_trade.id as trade_id,
        kpler_trade.flow_id,
        kpler_trade.product_id,
        'matching_ongoing__flow$dep$vessels$value' as reason
    from
        kpler_trade
        join last_update on
            kpler_trade.flow_id = last_update.flow_id
            and kpler_trade.departure_date_utc = last_update.departure_date_utc
            and kpler_trade.departure_zone_id = last_update.departure_zone_id
            and (
                kpler_trade.arrival_zone_id is NULL or
                kpler_trade.status = 'ongoing'
            )
            and kpler_trade.value_tonne = last_update.value_tonne
    where updated_on_max - updated_on >= '2 hours'
),

matching__flow$arr$dep$vessels as (
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
    select
        kpler_trade.id as trade_id,
        kpler_trade.flow_id,
        kpler_trade.product_id,
        'matching__flow$arr$dep$vessels' as reason
    from
        kpler_trade
        join last_update on
            kpler_trade.flow_id = last_update.flow_id
            and kpler_trade.departure_date_utc = last_update.departure_date_utc
            and kpler_trade.departure_zone_id = last_update.departure_zone_id
            and kpler_trade.vessel_imos = last_update.vessel_imos
            and kpler_trade.arrival_date_utc = last_update.arrival_date_utc
            and kpler_trade.arrival_zone_id = last_update.arrival_zone_id
    where (updated_on_max - updated_on >= '2 hours')
),

matching__flow$dep as (
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
    select
        kpler_trade.id as trade_id,
        kpler_trade.flow_id,
        kpler_trade.product_id,
        'matching__flow$dep' as reason
    from
        kpler_trade join
        last_update on
        kpler_trade.flow_id = last_update.flow_id
        and kpler_trade.departure_date_utc = last_update.departure_date_utc
        and kpler_trade.departure_zone_id = last_update.departure_zone_id
    where (updated_on_max - updated_on >= '2 hours')
),

matching__dep_country$product as (
    with sub as (
        select
            DATE(trade.departure_date_utc) as origin_date,
            zone.country_iso2 as country_iso2_departure,
            trade.product_id,
            MAX(trade.updated_on) OVER (
                PARTITION BY zone.country_iso2,
                trade.product_id
                ORDER BY
                    DATE(trade.departure_date_utc),
                    trade.updated_on ROWS BETWEEN UNBOUNDED PRECEDING
                    and CURRENT ROW
            ) as updated_on_max
        from
            kpler_trade as trade
            join kpler_zone as zone on trade.departure_zone_id = zone.id
    ),
    last_update as (
        select
            sub.origin_date,
            sub.country_iso2_departure,
            sub.product_id,
            MAX(sub.updated_on_max) as updated_on_max
        from
            sub
        group by
            sub.origin_date,
            sub.country_iso2_departure,
            sub.product_id
    )
    select
        kpler_trade.id as trade_id,
        kpler_trade.flow_id,
        kpler_trade.product_id,
        'matching__dep_country$product' as reason
    from
        kpler_trade
        join kpler_zone on kpler_trade.departure_zone_id = kpler_zone.id
        join last_update on kpler_zone.country_iso2 = last_update.country_iso2_departure
            and kpler_trade.product_id = last_update.product_id
            and DATE(kpler_trade.departure_date_utc) = last_update.origin_date
    WHERE
        last_update.updated_on_max - kpler_trade.updated_on >= interval '7 days'
)

select * from matching_id
    UNION
select * from matching__flow$dep$arr_zone$value
    UNION
select * from matching_ongoing__flow$dep$vessels$value
    UNION
select * from matching__flow$arr$dep$vessels
    UNION
select * from matching__flow$dep
    UNION
select * from matching__dep_country$product;


with trade_valid as (
    SELECT
        kpler_trade.id,
        kpler_trade.flow_id,
        kpler_trade.product_id,
        invalid_trade.trade_id is null as is_valid
    FROM
        kpler_trade join invalid_trade
            on kpler_trade.id = invalid_trade.trade_id
            and kpler_trade.flow_id = invalid_trade.flow_id
            and kpler_trade.product_id = invalid_trade.product_id
)

update
    kpler_trade
set
    is_valid = trade_valid.is_valid
from
    trade_valid
where kpler_trade.id = trade_valid.id
        and kpler_trade.flow_id = trade_valid.flow_id
        and kpler_trade.product_id = trade_valid.product_id;
