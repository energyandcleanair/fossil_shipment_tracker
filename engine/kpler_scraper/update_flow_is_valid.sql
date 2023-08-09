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
