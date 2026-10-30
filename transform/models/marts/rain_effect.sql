-- How much does rain cut riding?
--
-- Each wet hour is compared with dry hours at the same counter, in the same month
-- of the same year, on the same kind of day (weekday or weekend), at the same hour
-- of day. That holds season, long-term trends (including 2020), the weekly rhythm
-- and the daily rush hours constant, so what's left is mostly the weather. "Dry"
-- hours here are also dry for the three hours before, so wet pavement after a
-- shower doesn't count as dry.
--
-- change = observed riders in wet hours / riders expected from the matching dry hours - 1
with hours as (
    select *
    from {{ ref('fct_hourly') }}
    where bikes is not null
        and sky is not null
        and not is_fall_back_hour
        and hour_of_day between {{ var('day_start') }} and {{ var('day_end') }}
),

dry as (
    select counter_id, year, month, is_weekend, hour_of_day, avg(bikes) as dry_mean, count(*) as dry_hours
    from hours
    where sky = 'dry' and not rained_in_prev_3h
    group by all
),

wet as (
    select
        h.*,
        d.dry_mean,
        case
            when h.is_weekend then 'weekend'
            when h.hour_of_day in (7, 8, 9, 16, 17, 18) then 'weekday commute'
            else 'weekday other'
        end as daypart
    from hours h
    join dry d using (counter_id, year, month, is_weekend, hour_of_day)
    where h.sky in ('drizzle', 'rain') and d.dry_hours >= 3
)

select
    coalesce(counter_id, 'all') as counter_id,
    sky,
    coalesce(daypart, 'all') as daypart,
    count(*) as wet_hours,
    sum(bikes) as observed,
    round(sum(dry_mean), 1) as expected,
    round(sum(bikes) / nullif(sum(dry_mean), 0) - 1, 4) as change
from wet
group by grouping sets ((counter_id, sky, daypart), (counter_id, sky), (sky, daypart), (sky))
