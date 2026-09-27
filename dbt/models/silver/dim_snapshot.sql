-- One row per sampled snapshot, with its position in the sampling sequence and the gap since the previous one.
with snaps as (
    select
        snapshot_ts,
        count(*) as n_rows,
        count(speed_band) as n_rows_known_band
    from {{ ref('stg_speedband_obs') }}
    group by snapshot_ts
),

ordered as (
    select
        *,
        lag(snapshot_ts) over (order by snapshot_ts) as prev_snapshot_ts
    from snaps
),

gaps as (
    select
        *,
        {{ minutes_between('prev_snapshot_ts', 'snapshot_ts') }} as gap_minutes,
        coalesce({{ minutes_between('prev_snapshot_ts', 'snapshot_ts') }} > {{ var('max_gap_minutes') }}, true)
            as starts_sampling_run
    from ordered
)

select
    g.snapshot_ts,
    row_number() over (order by g.snapshot_ts) as snapshot_seq,
    sum(case when g.starts_sampling_run then 1 else 0 end)
        over (order by g.snapshot_ts rows between unbounded preceding and current row) as sampling_run,
    g.prev_snapshot_ts,
    round(g.gap_minutes, 2) as gap_minutes,
    g.n_rows,
    g.n_rows_known_band,
    cast(g.snapshot_ts as date) as obs_date,
    hour(g.snapshot_ts) as obs_hour,
    c.day_type
from gaps as g
left join {{ ref('dim_calendar') }} as c on c.cal_date = cast(g.snapshot_ts as date)
