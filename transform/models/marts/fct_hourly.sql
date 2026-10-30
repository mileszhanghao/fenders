-- The analysis table: one row per counter and local hour on days the counter was
-- working, with the weather for that hour and the three hours before it.
with weather as (
    select
        *,
        -- Pavement stays wet after the rain stops; riders notice.
        coalesce(max(precipitation_mm) over (
            order by hour_local rows between 3 preceding and 1 preceding
        ), 0) as max_precip_prev_3h_mm
    from {{ ref('stg_weather_hourly') }}
)

select
    h.counter_id,
    h.hour_local,
    h.date_local,
    h.hour_of_day,
    year(h.hour_local) as year,
    month(h.hour_local) as month,
    isodow(h.hour_local) >= 6 as is_weekend,
    h.bikes,
    h.pedestrians,
    h.is_fall_back_hour,
    w.precipitation_mm,
    w.temperature_c,
    w.wind_kmh,
    case
        when w.precipitation_mm is null then null
        when w.precipitation_mm < {{ var('drizzle_mm') }} then 'dry'
        when w.precipitation_mm < {{ var('rain_mm') }} then 'drizzle'
        else 'rain'
    end as sky,
    w.max_precip_prev_3h_mm >= {{ var('drizzle_mm') }} as rained_in_prev_3h
from {{ ref('int_counter_hourly') }} h
join {{ ref('int_counter_days') }} d using (counter_id, date_local)
left join weather w using (hour_local)
where not d.is_excluded
