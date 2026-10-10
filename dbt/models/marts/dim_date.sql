-- Calendar with the things that move South African retail: weekends, public holidays, month-end payday.
with bounds as (
    select
        least(coalesce(min(placed_at)::date, current_date), current_date - 90) as first_day,
        current_date + 30                                                     as last_day
    from {{ ref('stg_cdc__orders') }}
),
days as (
    select generate_series(first_day, last_day, interval '1 day')::date as date_day from bounds
)
select
    d.date_day,
    extract(isodow from d.date_day)::int              as day_of_week,     -- 1 = Monday
    to_char(d.date_day, 'Dy')                         as day_name,
    date_trunc('week', d.date_day)::date              as week_start,
    date_trunc('month', d.date_day)::date             as month_start,
    extract(isodow from d.date_day) in (6, 7)         as is_weekend,
    h.holiday_date is not null                        as is_public_holiday,
    h.holiday_name,
    (extract(day from d.date_day) >= 25 or extract(day from d.date_day) = 1) as is_payday_period
from days d
left join {{ ref('stg_public_holidays') }} h on h.holiday_date = d.date_day
