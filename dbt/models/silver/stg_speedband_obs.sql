-- One row per (snapshot, link). The bronze MERGE only inserts new keys, so bronze should already be unique; the
-- row_number keeps silver correct if a key is ever stored twice (for example a raw append that bypassed the MERGE):
-- the copy written first wins, as in bronze.
with src as (
    select * from {{ source('bronze', 'speedbands') }}
    {%- set cutoff = env_var('SNAPSHOT_CUTOFF_SGT', '') %}
    {% if cutoff %}
    -- Bronze may already contain newer snapshots from streaming or a later run.
    where snapshot_ts <= cast('{{ cutoff | replace("'", "''") }}' as timestamp)
    {% endif %}
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
    _source as ingest_source,
    _ingested_at as ingested_at_utc,
    _late as arrived_late
from ranked
where rn = 1
