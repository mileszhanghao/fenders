-- Open-Meteo hourly weather, moved onto the same local clock hours the counters use.
-- On the night clocks fall back, two UTC hours share one local hour (01:00); they are
-- combined, the same way the counters combine them. On the spring-forward night the
-- local 02:00 hour doesn't exist, and no UTC hour maps to it.
with utc as (
    select
        cast(time_utc as timestamp) as hour_utc,
        precipitation,
        temperature_2m,
        wind_speed_10m,
        cloud_cover
    from {{ lake('weather/*.parquet') }}
)

select
    timezone('{{ var("timezone") }}', timezone('UTC', hour_utc)) as hour_local,
    count(*) as utc_hours,
    sum(precipitation) as precipitation_mm,
    avg(temperature_2m) as temperature_c,
    avg(wind_speed_10m) as wind_kmh,
    avg(cloud_cover) as cloud_cover_pct,
    min(hour_utc) as first_hour_utc
from utc
group by 1
