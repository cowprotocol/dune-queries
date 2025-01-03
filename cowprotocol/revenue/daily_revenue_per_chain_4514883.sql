-- This query returns the daily revenue (per type) for a given target chain
-- Parameters:
--  {{ui_fee_recipient}} - the partner address that receives the CoW Swap UI fee
--  {{blockchain}} - the chain for which to collect the data

with prep as (
    select
        date(block_time) as "day",
        sum("Limit") as "Limit",
        sum("Market") as "Market",
        sum("UI Fee") as "UI Fee",
        sum("Partner Fee Share") as "Partner Fee Share"
    from "query_4217030(blockchain='{{blockchain}}',ui_fee_recipient='{{ui_fee_recipient}}')"
    group by 1
)

select
    day,
    type,
    value
from prep
cross join
    unnest(
        array["Limit", "Market", "UI Fee", "Partner Fee Share"],
        array["Limit", "Market", "UI Fee", "Partner Fee Share"]
    )
order by 1 desc
