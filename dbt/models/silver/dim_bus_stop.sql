with src as (select * from {{ source('bronze', 'bus_stops') }}),
latest as (select max(as_of) as as_of from src)
select
    BusStopCode as stop_code,
    Description as stop_name,
    RoadName as road_name,
    Latitude as lat,
    Longitude as lon,
    src.as_of
from src join latest using (as_of)
