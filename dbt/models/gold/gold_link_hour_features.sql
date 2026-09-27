-- ML-ready table: one row per link and clock hour with at least one known speed band.
-- Features describe the hour; next_hour_mean_band is a label for a one-hour-ahead model and is NULL when the next
-- clock hour was not sampled. Variance comes from integer sums (reproducible, see gold_link_speed_hourly).
with hourly as (
    select
        o.link_id,
        date_trunc('hour', o.snapshot_ts) as obs_hour_ts,
        any_value(s.day_type) as day_type,
        count(*) as n_obs,
        sum(o.speed_band) as s1,
        sum(o.speed_band * o.speed_band) as s2,
        min(o.speed_band) as min_band,
        count(*) filter (where o.speed_band <= {{ var('congestion_band_max') }}) as n_congested
    from {{ ref('stg_speedband_obs') }} as o
    join {{ ref('dim_snapshot') }} as s using (snapshot_ts)
    where o.speed_band is not null
    group by o.link_id, date_trunc('hour', o.snapshot_ts)
),

features as (
    select
        link_id,
        obs_hour_ts,
        hour(obs_hour_ts) as hour_of_day,
        isodow(obs_hour_ts) as iso_dow,
        day_type = 'WEEKENDS/HOLIDAY' as is_weekend_or_holiday,
        n_obs,
        round(s1 / n_obs, 4) as mean_band,
        min_band,
        case when n_obs > 1 then round(sqrt(greatest((s2 - s1 * s1 / n_obs) / (n_obs - 1), 0)), 4) end as band_std,
        round(n_congested / n_obs, 4) as share_congested
    from hourly
),

with_neighbours as (
    select
        *,
        lag(obs_hour_ts) over w as prev_hour_ts,
        lag(mean_band) over w as prev_mean_band,
        lead(obs_hour_ts) over w as next_hour_ts,
        lead(mean_band) over w as next_mean_band
    from features
    window w as (partition by link_id order by obs_hour_ts)
)

select
    f.link_id,
    f.obs_hour_ts,
    f.hour_of_day,
    f.iso_dow,
    f.is_weekend_or_holiday,
    l.road_category,
    l.length_m,
    f.n_obs,
    f.mean_band,
    f.min_band,
    f.band_std,
    f.share_congested,
    case when f.prev_hour_ts = f.obs_hour_ts - interval 1 hour then f.prev_mean_band end as prev_hour_mean_band,
    case when f.next_hour_ts = f.obs_hour_ts + interval 1 hour then f.next_mean_band end as next_hour_mean_band
from with_neighbours as f
join {{ ref('dim_link') }} as l using (link_id)
