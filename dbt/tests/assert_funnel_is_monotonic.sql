-- A funnel can only narrow: sessions >= viewed >= added >= checkout >= purchased.
{{ config(severity='warn') }}
select *
from {{ ref('mart_funnel_daily') }}
where not (added_to_cart >= reached_checkout and reached_checkout >= purchased)
