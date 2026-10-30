-- One row per counter and day, for charts. Excluded days stay in with a flag (and
-- no count), so a chart shows a gap instead of a misleading drop to zero.
with weather as (
    select
        cast(hour_local as date) as date_local,
        sum(precipitation_mm) as precipitation_mm,
        max(temperature_c) as high_c
    from {{ ref('stg_weather_hourly') }}
    group by 1
)

select
    d.counter_id,
    d.date_local,
    case when not d.is_excluded then d.bikes end as bikes,
    d.is_excluded,
    d.is_incomplete,
    d.is_suspect_zero,
    round(w.precipitation_mm, 1) as precipitation_mm,
    round(w.high_c, 1) as high_c
from {{ ref('int_counter_days') }} d
left join weather w using (date_local)
