-- This a base query for computing token imbalances of the CoW Protocol contract
--
-- It is based on query 4021257 and aggregates the transfers from that query into imbalances per token
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on

select
    block_time,
    tx_hash,
    token_address,
    sum(case when receiver = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 then cast(amount as int256) else -cast(amount as int256) end) as amount -- this classifies transfers from the settlement contract to itself as incoming transfers
from "query_4021257(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
group by block_time, tx_hash, token_address
