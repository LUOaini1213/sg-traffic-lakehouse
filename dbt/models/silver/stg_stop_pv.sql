select
    YEAR_MONTH as year_month,
    DAY_TYPE as day_type,
    TIME_PER_HOUR as hour,
    PT_CODE as stop_code,
    TOTAL_TAP_IN_VOLUME as tap_in,
    TOTAL_TAP_OUT_VOLUME as tap_out
from {{ source('bronze', 'pv_stop') }}
