-- DataMall monthly passenger volumes are sums over all days of a type; dividing by these counts gives per-day values.
select year_month, day_type, count(*) as n_days
from {{ ref('dim_calendar') }}
group by year_month, day_type
