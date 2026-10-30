-- If our sum disagrees with a counter's own published total on more than 5% of
-- hours, the column mapping is wrong (for example a total mapped as a direction).
{{ config(severity='error') }}
{{ totals_mismatch(0.05) }}
