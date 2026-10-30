-- The counters publish their own total next to the per-direction counts. If our sum
-- disagrees with it on more than 1% of hours, the column mapping is probably wrong
-- (for example a "total" column mapped as a direction).
{{ config(severity='error') }}
with checked as (
    select
        counter_id,
        count(*) as hours,
        count(*) filter (where published_total <> coalesce(bikes, 0) + coalesce(pedestrians, 0)) as mismatched
    from {{ ref('int_counter_hourly') }}
    where published_total is not null and bikes is not null
    group by 1
)
select * from checked where mismatched > 0.01 * hours
