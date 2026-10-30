-- One row per counter and local clock hour: bikes, pedestrians, and the counter's
-- own published total for cross-checking.
with mapped as (
    select r.*, m.role, m.mode
    from {{ ref('stg_counter_readings') }} r
    left join {{ ref('counter_columns') }} m using (counter_id, source_column)
),

expected as (
    select counter_id, count(*) filter (where mode = 'bike') as bike_parts
    from {{ ref('counter_columns') }}
    where role = 'part'
    group by 1
),

per_hour as (
    select
        counter_id,
        hour_local,
        sum(value) filter (where mode = 'bike') as bike_sum,
        count(value) filter (where mode = 'bike') as bike_parts_reported,
        sum(value) filter (where mode = 'ped') as pedestrians,
        max(value) filter (where role = 'total') as published_total,
        count(*) filter (where role is null) as unmapped_values,
        max(loaded_at) as loaded_at
    from mapped
    group by 1, 2
)

select
    p.counter_id,
    p.hour_local,
    cast(p.hour_local as date) as date_local,
    hour(p.hour_local) as hour_of_day,
    -- A bike count is only trusted when every direction reported.
    case when p.bike_parts_reported = e.bike_parts then p.bike_sum end as bikes,
    p.pedestrians,
    p.published_total,
    p.unmapped_values,
    -- The night clocks fall back, the counter's 01:00 row holds two real hours.
    (month(p.hour_local) = 11 and hour(p.hour_local) = 1
        and p.hour_local::date = (
            -- first Sunday of November
            make_date(year(p.hour_local), 11, 1)
            + to_days(cast((7 - dayofweek(make_date(year(p.hour_local), 11, 1))) % 7 as integer))
        )) as is_fall_back_hour,
    p.loaded_at
from per_hour p
join expected e using (counter_id)
