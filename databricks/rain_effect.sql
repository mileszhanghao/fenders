-- Spark SQL port of transform/models/marts/rain_effect.sql, for Databricks.
-- Reads the `fct_hourly` table/view (the mart published to S3 as Parquet) and gives the
-- same answer as the dbt model on DuckDB; tests/test_databricks.py checks they match.
--
-- Each wet hour is compared with dry hours at the same counter, in the same month of the
-- same year, on the same kind of day, at the same hour of day (dry = dry for the 3 hours
-- before as well). change = observed riders / riders expected from those dry hours - 1.
create or replace temporary view rain_effect as
with hours as (
    select *
    from fct_hourly
    where bikes is not null
        and sky is not null
        and not is_fall_back_hour
        and hour_of_day between 6 and 21
),

dry as (
    select counter_id, year, month, is_weekend, hour_of_day, avg(bikes) as dry_mean, count(*) as dry_hours
    from hours
    where sky = 'dry' and not rained_in_prev_3h
    group by counter_id, year, month, is_weekend, hour_of_day
),

wet as (
    select
        h.counter_id,
        h.year,
        h.sky,
        h.bikes,
        d.dry_mean,
        case
            when h.is_weekend then 'weekend'
            when h.hour_of_day in (7, 8, 9, 16, 17, 18) then 'weekday commute'
            else 'weekday other'
        end as daypart
    from hours h
    join dry d
        on h.counter_id = d.counter_id
        and h.year = d.year
        and h.month = d.month
        and h.is_weekend = d.is_weekend
        and h.hour_of_day = d.hour_of_day
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
group by grouping sets ((counter_id, sky, daypart), (counter_id, sky), (sky, daypart), (sky));

-- Beyond the original model: has the rain penalty changed over the years?
create or replace temporary view rain_effect_by_year as
with hours as (
    select *
    from fct_hourly
    where bikes is not null and sky is not null and not is_fall_back_hour and hour_of_day between 6 and 21
),
dry as (
    select counter_id, year, month, is_weekend, hour_of_day, avg(bikes) as dry_mean, count(*) as dry_hours
    from hours
    where sky = 'dry' and not rained_in_prev_3h
    group by counter_id, year, month, is_weekend, hour_of_day
)
select
    h.year,
    count(*) as wet_hours,
    round(sum(h.bikes) / nullif(sum(d.dry_mean), 0) - 1, 4) as change
from hours h
join dry d
    on h.counter_id = d.counter_id and h.year = d.year and h.month = d.month
    and h.is_weekend = d.is_weekend and h.hour_of_day = d.hour_of_day
where h.sky = 'rain' and d.dry_hours >= 3
group by h.year;
