-- One value per counter, local hour and column.
--
-- The published data sometimes has more than one row for the same hour:
-- * exact repeats (Burke-Gilman published September 2020 twice): kept once;
-- * one row empty and one with values: the values are used;
-- * two rows with different values (Spokane St, 2022-2025): there is no way to
--   tell which is right, so the value is left empty and the hour is flagged.
--   Adding them up would double count.
select
    counter_id,
    hour_local,
    source_column,
    case when count(distinct value) <= 1 then max(value) end as value,
    count(*) as rows_published,
    count(distinct value) > 1 as is_conflicting,
    max(loaded_at) as loaded_at
from {{ ref('stg_counter_readings') }}
group by 1, 2, 3
