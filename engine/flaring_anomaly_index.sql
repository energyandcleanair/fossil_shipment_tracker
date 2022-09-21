with stats as
(select facility_id, avg(value) as mean, stddev(value) as stddev
 from flaring
 group by 1),

scores as (
    select  flaring.facility_id, DATE_PART('year', date) as year, (value - mean) / stddev as zscore
     from flaring
     left join stats on stats.facility_id = flaring.facility_id
     where DATE_PART('doy', date) >= DATE_PART('doy', '2022-02-24'::timestamp)
     and DATE_PART('doy', date) <= DATE_PART('doy', (select max(date) from flaring))
      AND ( :facility_id IS NULL OR flaring.facility_id=ANY( :facility_id))
),

year_scores as (
    select facility_id, (year=2022) as current_year, avg(zscore) as zscore
    from scores
    group by 1,2
),

year_scores_wide as (
    select facility_id,
       max(case when (current_year) then zscore else NULL end) as current_year,
        max(case when (not current_year) then zscore else NULL end) as not_current_year
    from year_scores
    group by 1
)

select facility_id, name, type, ST_AsBinary(geometry), current_year-not_current_year as anomaly_index
from year_scores_wide
left join flaring_facility on facility_id=flaring_facility.id




