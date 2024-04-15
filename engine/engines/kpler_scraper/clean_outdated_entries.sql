update
    kpler_trade
set
    is_valid = case
        when kpler_sync_history.last_updated is not null then kpler_sync_history.last_updated = kpler_trade.updated_on
        else false
    end
from
    kpler_trade as trades
    left join kpler_zone departure_zone on trades.departure_zone_id = departure_zone.id
    left join kpler_sync_history on date_trunc('day', kpler_sync_history.date) = date_trunc('day', trades.departure_date_utc)
    and kpler_sync_history.country_iso2 = departure_zone.country_iso2
where
    kpler_trade.id = trades.id
    and kpler_trade.flow_id = trades.flow_id
    and kpler_trade.product_id = trades.product_id;
