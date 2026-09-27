-- Every day of every year that the data touches: from 1 January of the earliest year in the speed bands, stop
-- volumes or OD trips to 31 December of the latest. Derived from the data, so a file from a new year is never
-- dropped by the inner joins downstream (tests/assert_gold_keeps_every_silver_row.sql would fail if it were).
-- Public holidays come from the seed; tests/assert_calendar_years_have_holidays.sql warns for a year it lacks.
with data_dates as (
    select min(cast(snapshot_ts as date)) as lo, max(cast(snapshot_ts as date)) as hi
    from {{ ref('stg_speedband_obs') }}
    union all
    select min(cast(strptime(year_month, '%Y-%m') as date)), max(cast(strptime(year_month, '%Y-%m') as date))
    from {{ ref('stg_stop_pv') }}
    union all
    select min(cast(strptime(year_month, '%Y-%m') as date)), max(cast(strptime(year_month, '%Y-%m') as date))
    from {{ ref('stg_od') }}
),

bounds as (
    select make_date(year(min(lo)), 1, 1) as lo, make_date(year(max(hi)), 12, 31) as hi
    from data_dates
),

days as (
    select cast(unnest(generate_series(lo, hi, interval 1 day)) as date) as d
    from bounds
)

select
    d as cal_date,
    isodow(d) as iso_dow,
    h.holiday_name,
    case when isodow(d) >= 6 or h.holiday_date is not null then 'WEEKENDS/HOLIDAY' else 'WEEKDAY' end as day_type,
    strftime(d, '%Y-%m') as year_month
from days
left join {{ ref('public_holidays_sg') }} as h on h.holiday_date = d
