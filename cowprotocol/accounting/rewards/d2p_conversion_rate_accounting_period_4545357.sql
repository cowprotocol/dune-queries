-- Query 4545357 refactored to use 5533118
-- No changes needed here when new chains are added to 5533118

select
    timestamp '{{start_time}}' as start_time,
    timestamp '{{end_time}}' as end_time,
    cow_price / native_token_price as conversion_rate_cow_to_native
from "query_5533118"
where
    blockchain = '{{blockchain}}'
    and end_time = date_trunc('day', timestamp '{{end_time}}')

