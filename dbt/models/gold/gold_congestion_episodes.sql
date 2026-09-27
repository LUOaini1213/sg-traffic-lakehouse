-- Congestion episode: a run of at least `congestion_min_snapshots` consecutive snapshots in which a link on a
-- major road reports band <= `congestion_band_max`. Consecutive means adjacent in the global snapshot sequence and
-- in the same sampling run, so an episode never bridges a sampling gap or a snapshot where the link is missing.
with obs as (
    select
        o.link_id,
        o.snapshot_ts,
        o.speed_band,
        s.snapshot_seq,
        s.sampling_run
    from {{ ref('stg_speedband_obs') }} as o
    join {{ ref('dim_snapshot') }} as s using (snapshot_ts)
    join {{ ref('dim_link') }} as l using (link_id)
    where o.speed_band <= {{ var('congestion_band_max') }}
      and l.road_category in ({{ var('congestion_road_categories') | join(', ') }})
),

islands as (
    -- gaps-and-islands: consecutive snapshot_seq values of a link share one island number
    select
        *,
        snapshot_seq - row_number() over (partition by link_id order by snapshot_seq) as island
    from obs
)

select
    md5(cast(link_id as varchar) || '|' || strftime(min(snapshot_ts), '%Y%m%dT%H%M')) as episode_id,
    link_id,
    min(snapshot_ts) as start_ts,
    max(snapshot_ts) as end_ts,
    count(*) as n_snapshots,
    min(speed_band) as worst_band,
    round({{ minutes_between('min(snapshot_ts)', 'max(snapshot_ts)') }}, 1) as span_minutes
from islands
group by link_id, sampling_run, island  -- sampling_run splits an island at a sampling gap
having count(*) >= {{ var('congestion_min_snapshots') }}
