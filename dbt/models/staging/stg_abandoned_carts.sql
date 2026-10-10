select session_id, customer_id, cart_value, items, last_activity_at, detected_at
from {{ source('raw', 'abandoned_carts') }}
