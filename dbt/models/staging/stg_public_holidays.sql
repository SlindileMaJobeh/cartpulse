select holiday_date, name as holiday_name, source
from {{ source('raw', 'public_holidays') }}
