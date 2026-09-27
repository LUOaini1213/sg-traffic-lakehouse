-- Every bronze speed-band row must sit in the obs_date / obs_hour partition of its own event time. A row in the wrong
-- partition means the partitioning code used arrival time (or a wrong clock) instead of event time.
select snapshot_ts, link_id, partition_date, partition_hour
from {{ ref('stg_speedband_obs') }}
where partition_date <> cast(snapshot_ts as date)
   or partition_hour <> hour(snapshot_ts)
