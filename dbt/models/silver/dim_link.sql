select
    l.LinkID as link_id,
    l.RoadName as road_name,
    l.RoadCategory as road_category,
    r.label as road_category_label,
    l.StartLat as start_lat,
    l.StartLon as start_lon,
    l.EndLat as end_lat,
    l.EndLon as end_lon,
    -- haversine length in metres
    round(2 * 6371008.8 * asin(sqrt(
        pow(sin(radians(l.EndLat - l.StartLat) / 2), 2)
        + cos(radians(l.StartLat)) * cos(radians(l.EndLat)) * pow(sin(radians(l.EndLon - l.StartLon) / 2), 2)
    )), 1) as length_m
from {{ source('bronze', 'speedband_links') }} as l
left join {{ ref('road_category_ref') }} as r on r.road_category = l.RoadCategory
