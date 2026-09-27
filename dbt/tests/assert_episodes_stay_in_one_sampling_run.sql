-- An episode is a run of consecutive snapshots, so it must start and end in the same sampling run (it never
-- bridges a sampling gap), and its snapshot count must equal the number of snapshots between its start and end.
select e.episode_id, e.n_snapshots, s1.sampling_run as start_run, s2.sampling_run as end_run,
       s2.snapshot_seq - s1.snapshot_seq + 1 as snapshots_spanned
from {{ ref('gold_congestion_episodes') }} as e
join {{ ref('dim_snapshot') }} as s1 on s1.snapshot_ts = e.start_ts
join {{ ref('dim_snapshot') }} as s2 on s2.snapshot_ts = e.end_ts
where s1.sampling_run <> s2.sampling_run
   or s2.snapshot_seq - s1.snapshot_seq + 1 <> e.n_snapshots
