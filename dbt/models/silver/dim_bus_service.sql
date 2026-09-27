with src as (select * from {{ source('bronze', 'bus_services') }}),
latest as (select max(as_of) as as_of from src)
select
    ServiceNo as service_no,
    Direction as direction,
    Operator as operator,
    Category as category,
    OriginCode as origin_stop_code,
    DestinationCode as destination_stop_code,
    AM_Peak_Freq as am_peak_headway_min,
    src.as_of
from src join latest using (as_of)
