-- Tap-ins and tap-outs per stop, hour and day type, with per-day averages. DataMall publishes monthly sums over
-- all days of a type, and months differ in how many weekdays they have (August 2026 has National Day in lieu).
select
    p.year_month,
    p.day_type,
    p.hour,
    p.stop_code,
    st.stop_name,
    p.tap_in,
    p.tap_out,
    d.n_days,
    round(p.tap_in / d.n_days, 2) as avg_daily_tap_in,
    round(p.tap_out / d.n_days, 2) as avg_daily_tap_out
from {{ ref('stg_stop_pv') }} as p
join {{ ref('dim_month_day_counts') }} as d using (year_month, day_type)
left join {{ ref('dim_bus_stop') }} as st using (stop_code)
