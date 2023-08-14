--KPLER sometimes changes destination or commodity of recent flows / trades
--This script is here to ensure that we don't double count those that have been updated
--by declaring "invalid" the flows that have been updated since then

-- FLOWS
with last_update as (
	select from_zone_id, from_split, to_split, date,
            platform, max(updated_on) as updated_on_max
    from kpler_flow
    group by from_zone_id, from_split, to_split, date, platform
)

update kpler_flow
set is_valid = updated_on_max - updated_on < '15 minutes'
from last_update
where kpler_flow.from_zone_id = last_update.from_zone_id
and kpler_flow.from_split = last_update.from_split
and kpler_flow.to_split = last_update.to_split
and kpler_flow.date = last_update.date
and kpler_flow.platform = last_update.platform;

-- TRADES
with last_update as (
	select id, max(updated_on) as updated_on_max
    from kpler_trade
    group by 1
)

update kpler_trade
set is_valid = updated_on_max - updated_on < '15 minutes'
from last_update
where kpler_trade.id = last_update.id;
