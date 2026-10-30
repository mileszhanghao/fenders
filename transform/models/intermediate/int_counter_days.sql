-- Daily health check per counter. Counters go offline (construction, a failed
-- sensor, a trail closure) and report zeros or nothing instead of real traffic;
-- those days would look exactly like "nobody rode" if they weren't flagged.
with daily as (
    select
        counter_id,
        date_local,
        count(bikes) as hours_reported,
        sum(bikes) as bikes,
        count(*) filter (where is_conflicting) as conflicting_hours,
        count(*) filter (where is_spike) as spike_hours,
        count(*) filter (
            where bikes = 0 and hour_of_day between {{ var('day_start') }} + 1 and {{ var('day_end') }} - 2
        ) as daytime_zero_hours
    from {{ ref('int_counter_hourly') }}
    group by 1, 2
)

select
    *,
    -- 23 allows for the spring-forward day, whose 02:00 hour doesn't exist. Hours
    -- with conflicting duplicate rows count as missing.
    hours_reported < 23 as is_incomplete,
    -- Eight or more daytime hours of exactly zero: the busiest trails in a city of
    -- 750,000 don't go empty for a whole working day, even in snow.
    daytime_zero_hours >= 8 as is_suspect_zero,
    hours_reported < 23
    {%- if var('exclude_zero_days') %} or daytime_zero_hours >= 8{% endif %} as is_excluded
from daily
