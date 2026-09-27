select
    cast(d as date) as cal_date,
    isodow(d) as iso_dow,
    h.holiday_name,
    case when isodow(d) >= 6 or h.holiday_date is not null then 'WEEKENDS/HOLIDAY' else 'WEEKDAY' end as day_type,
    strftime(d, '%Y-%m') as year_month
from generate_series(date '{{ var("calendar_start") }}', date '{{ var("calendar_end") }}', interval 1 day) as t(d)
left join {{ ref('public_holidays_sg_2026') }} as h on h.holiday_date = cast(d as date)
