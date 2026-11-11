-- COW to SOL conversion rate of a Solana accounting period (Solana version of 4545357)
-- Parameters:
--  {{start_time}} - the start date timestamp for the accounting period (inclusively)
--  {{end_time}}   - the end date timestamp for the accounting period (exclusively)
--  {{price_date}} - empty: the official rate of the period; a day before end_time: the provisional rate of that day

select
    timestamp '{{start_time}}' as start_time,
    timestamp '{{end_time}}' as end_time,
    cow_price / native_token_price as conversion_rate_cow_to_native,
    cast(end_time as timestamp) as price_end_time
from "query_5533118"
where
    blockchain = 'solana'
    and end_time = date_trunc(
        'day',
        least(
            cast(coalesce(nullif('{{price_date}}', ''), '{{end_time}}') as timestamp),
            timestamp '{{end_time}}'
        )
    )
