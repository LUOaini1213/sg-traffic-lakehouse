-- Two episodes of the same link must not overlap in time.
select a.episode_id, b.episode_id as other_episode_id
from {{ ref('gold_congestion_episodes') }} as a
join {{ ref('gold_congestion_episodes') }} as b
  on a.link_id = b.link_id and a.episode_id < b.episode_id
 and a.start_ts <= b.end_ts and b.start_ts <= a.end_ts
