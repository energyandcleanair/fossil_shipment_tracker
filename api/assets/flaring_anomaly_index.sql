with stats as
(select facility_id,
  avg(value) as mean,
  greatest(stddev(value), 1000) as stddev
 from flaring
 where date <= '2022-02-24'
 group by 1),

scores as (
    select  flaring.facility_id,
    (date >= '2022-02-24'::timestamp) as is_after,
      case
        when stddev != 0 then (value - mean) / stddev
        else 1000 end as zscore
     from flaring
     left join stats on stats.facility_id = flaring.facility_id
    WHERE ( :facility_id IS NULL OR flaring.facility_id=ANY( :facility_id))
),

period_scores as (
    select facility_id, is_after, avg(zscore) as zscore
    from scores
    group by 1,2
),

period_scores_wide as (
    select facility_id,
       max(case when (is_after) then zscore else NULL end) as after,
        max(case when (not is_after) then zscore else NULL end) as before
    from period_scores
    group by 1
)

select facility_id, name, name_en, type, url, ST_AsBinary(geometry), after-before as anomaly_index
from period_scores_wide
left join flaring_facility on facility_id=flaring_facility.id
