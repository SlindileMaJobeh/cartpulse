-- SCD type 2 from CDC: every change (moved province, opted in) is a new version.
with versions as (
    select *, {{ scd2_window('customer_id') }}
    from {{ ref('stg_cdc__customers') }}
)
select
    md5(customer_id::text || '-' || version::text)                    as customer_sk,
    customer_id,
    version,
    first_name,
    left(last_name, 1) || '.'                                         as last_initial,
    {{ mask_email('email') }}                                         as email_masked,
    {{ mask_phone('phone') }}                                         as phone_masked,
    province,
    city,
    marketing_opt_in,
    created_at,
    -- first version is open-ended to the past, so point-in-time joins never miss a
    -- row created a few microseconds after the fact that references it
    case when version = 1 then timestamptz '1900-01-01 00:00:00+00' else changed_at end as valid_from,
    coalesce(next_changed_at, timestamptz '9999-12-31 00:00:00+00')  as valid_to,
    next_changed_at is null and not is_deleted                       as is_current
from versions
