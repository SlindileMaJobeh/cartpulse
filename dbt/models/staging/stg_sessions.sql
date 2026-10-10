select
    session_id,
    customer_id,
    session_date,
    started_at,
    ended_at,
    duration_s,
    events,
    page_views,
    product_views,
    add_to_carts,
    checkouts,
    purchases,
    late_events,
    device,
    traffic_source,
    landing_page
from {{ source('raw', 'sessions') }}
