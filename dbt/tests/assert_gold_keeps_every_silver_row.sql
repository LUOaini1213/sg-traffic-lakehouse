-- Row-count parity between silver and the gold marts built from it with inner joins. A join key missing from a
-- dimension (for example a month outside the calendar) would otherwise drop rows without any error.
with checks as (
    select 'gold_stop_demand_hourly vs stg_stop_pv' as check_name,
           (select count(*) from {{ ref('stg_stop_pv') }}) as silver_rows,
           (select count(*) from {{ ref('gold_stop_demand_hourly') }}) as gold_rows
    union all
    select 'gold_link_speed_hourly n_obs vs known-band observations',
           (select count(*) from {{ ref('stg_speedband_obs') }} where speed_band is not null),
           (select coalesce(sum(n_obs), 0) from {{ ref('gold_link_speed_hourly') }})
    union all
    select 'OD month/day types without a day count',
           0,
           (select count(*) from (select distinct year_month, day_type from {{ ref('stg_od') }}) as o
            anti join {{ ref('dim_month_day_counts') }} as d using (year_month, day_type))
)
select * from checks where silver_rows <> gold_rows
