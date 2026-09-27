with src as (select * from {{ source('bronze', 'bus_routes') }}),
latest as (select max(as_of) as as_of from src)
select
    ServiceNo as service_no,
    Direction as direction,
    StopSequence as stop_sequence,
    BusStopCode as stop_code,
    Distance as distance_km,
    src.as_of
from src join latest using (as_of)
