{{ config(materialized='view') }}
-- A view: about 6 million rows a month that only the OD marts aggregate, so they stay in bronze Parquet.
select
    YEAR_MONTH as year_month,
    DAY_TYPE as day_type,
    TIME_PER_HOUR as hour,
    ORIGIN_PT_CODE as origin_stop_code,
    DESTINATION_PT_CODE as destination_stop_code,
    TOTAL_TRIPS as trips
from {{ source('bronze', 'pv_od') }}
