-- This query returns the daily revenue grouped by solver for a specific target chain
-- Parameters:
--  {{ui_fee_recipient}} - the partner address that receives the CoW Swap UI fee
--  {{blockchain}} - the chain for which to collect the data

select
    s.name,
    date(block_time) as "day",
    coalesce(sum("Limit"), 0) + coalesce(sum("Market"), 0) + coalesce(sum("UI Fee"), 0) + coalesce(sum("Partner Fee Share"), 0) as total
from "query_4217030(blockchain='{{blockchain}}',ui_fee_recipient='{{ui_fee_recipient}}')" as r
left join cow_protocol_{{blockchain}}.solvers as s
    on r.solver = s.address
group by 1, 2
