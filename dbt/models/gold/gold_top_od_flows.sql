-- The busiest stop-to-stop flows per month and day type, summed over all hours.
with flows as (
    select year_month, day_type, origin_stop_code, destination_stop_code, sum(trips) as trips
    from {{ ref('stg_od') }}
    group by year_month, day_type, origin_stop_code, destination_stop_code
),

ranked as (
    select
        *,
        row_number() over (
            partition by year_month, day_type
            order by trips desc, origin_stop_code, destination_stop_code
        ) as flow_rank
    from flows
)

select
    r.year_month,
    r.day_type,
    r.flow_rank,
    r.origin_stop_code,
    o.stop_name as origin_stop_name,
    r.destination_stop_code,
    d.stop_name as destination_stop_name,
    r.trips,
    c.n_days,
    round(r.trips / c.n_days, 1) as avg_daily_trips
from ranked as r
join {{ ref('dim_month_day_counts') }} as c using (year_month, day_type)
left join {{ ref('dim_bus_stop') }} as o on o.stop_code = r.origin_stop_code
left join {{ ref('dim_bus_stop') }} as d on d.stop_code = r.destination_stop_code
where r.flow_rank <= {{ var('top_od_n') }}
