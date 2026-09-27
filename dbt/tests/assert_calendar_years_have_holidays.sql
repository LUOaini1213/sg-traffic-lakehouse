-- A calendar year without public holidays in the seed would count holidays as weekdays, so per-day averages for
-- that year would be slightly off. Warn (the build continues); add the MOM list for the year to fix it.
{{ config(severity='warn') }}
select distinct year(c.cal_date) as calendar_year
from {{ ref('dim_calendar') }} as c
where year(c.cal_date) not in (select distinct year(holiday_date) from {{ ref('public_holidays_sg') }})
