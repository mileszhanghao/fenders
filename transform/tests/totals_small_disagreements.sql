-- Small disagreements are the publisher's own: Spokane St's total differs from
-- east + west by a few riders in about 2% of hours (mostly 2024). Reported, not fatal.
{{ config(severity='warn') }}
{{ totals_mismatch(0.01) }}
