-- One row per counter, local clock hour and published column, straight from the lake.
-- Counters publish different column sets; they are unpivoted here and given meaning
-- by the counter_columns seed in the next model.
with raw as (
    select
        regexp_extract(filename, 'counters/([^/]+)/', 1) as counter_id,
        -- Counters publish either `date` or `time`; both are local clock time with no zone.
        coalesce(try_cast("date" as timestamp), try_cast("time" as timestamp)) as reading_at,
        _loaded_at,
        * exclude (filename, "date", "time", _loaded_at)
    from {{ lake('counters/*/*.parquet') }}
),

long as (
    unpivot raw
    on columns(* exclude (counter_id, reading_at, _loaded_at))
    into name source_column value raw_value
)

select
    counter_id,
    date_trunc('hour', reading_at) as hour_local,
    source_column,
    -- Values arrive as text; anything that isn't a whole number is treated as missing.
    try_cast(raw_value as integer) as value,
    raw_value,
    cast(_loaded_at as timestamp) as loaded_at
from long
where reading_at is not null
