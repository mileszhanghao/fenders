{% macro totals_mismatch(rate) %}
-- Counters whose published total disagrees with the sum of their parts on more than
-- {{ rate * 100 }}% of hours.
with checked as (
    select
        counter_id,
        count(*) as hours,
        count(*) filter (where published_total <> coalesce(bikes, 0) + coalesce(pedestrians, 0)) as mismatched
    from {{ ref('int_counter_hourly') }}
    where published_total is not null and bikes is not null
    group by 1
)
select *, round(100.0 * mismatched / hours, 2) as pct from checked where mismatched > {{ rate }} * hours
{% endmacro %}
