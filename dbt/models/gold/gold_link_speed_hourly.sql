-- Speed-band profile of each link by hour of day and day type, over every sampled snapshot.
-- Averages are computed from integer sums, so the result does not depend on the order DuckDB's threads add rows
-- (a floating-point sum would, and the gold layer must be reproducible to the byte).
select
    o.link_id,
    s.day_type,
    s.obs_hour as hour_of_day,
    count(*) as n_obs,
    count(distinct s.obs_date) as n_days,
    round(sum(o.speed_band) / count(*), 4) as mean_band,
    min(o.speed_band) as min_band,
    max(o.speed_band) as max_band,
    round(count(*) filter (where o.speed_band <= {{ var('congestion_band_max') }}) / count(*), 4) as share_congested,
    round(sum(o.speed_lower_kmh) / count(*), 2) as mean_speed_lower_kmh
from {{ ref('stg_speedband_obs') }} as o
join {{ ref('dim_snapshot') }} as s using (snapshot_ts)
where o.speed_band is not null
group by o.link_id, s.day_type, s.obs_hour
