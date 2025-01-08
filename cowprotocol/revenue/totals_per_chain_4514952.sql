-- This query returns the total revenue (per type) for a given target chain
-- Parameters:
--  {{ui_fee_recipient}} - the partner address that receives the CoW Swap UI fee
--  {{blockchain}} - the chain for which to collect the data

with per_type as (
    select
        type,
        sum(value) as total
    from "query_4514883(blockchain='{{blockchain}}',ui_fee_recipient='{{ui_fee_recipient}}')"
    group by 1
)

select * from per_type
union distinct
select
    'Total' as "type",
    sum(total) as total
from per_type
order by 1 desc
