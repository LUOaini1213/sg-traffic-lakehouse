-- One row per (snapshot, link). Bronze is already unique per key (merge-on-write); the row_number keeps silver
-- correct even if two bronze objects ever held the same snapshot.
with src as (
    select * from {{ source('bronze', 'speedbands') }}
),

ranked as (
    select
        *,
        row_number() over (partition by snapshot_ts, link_id order by _ingested_at, _source) as rn
    from src
)

select
    snapshot_ts,
    link_id,
    case when speed_band between 1 and 8 then speed_band end as speed_band,
    coalesce(speed_band not between 1 and 8, true) as is_band_unknown,
    min_speed as speed_lower_kmh,
    case when max_speed = 999 then null else max_speed end as speed_upper_kmh,
    obs_date as partition_date,
    obs_hour as partition_hour,
    _source as ingest_source
from ranked
where rn = 1
