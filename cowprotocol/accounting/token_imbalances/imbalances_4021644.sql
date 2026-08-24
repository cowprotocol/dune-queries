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
    sum( -- noqa: PRS
        case
            when sender = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 and receiver = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 then cast(0 as int256)
            when receiver = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 then cast(amount as int256)
            else -cast(amount as int256)
        end
    ) as amount
from "query_4021257(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
-- Spam tokens emit fake Transfer events with amounts of up to 2^256 - 1, which do not fit into
-- int256. Amounts of 2^255 and larger cannot come from real trades and are ignored.
where amount < uint256 '57896044618658097711785492504343953926634992332820282019728792003956564819968' --noqa: PRS, LT02
group by block_time, tx_hash, token_address
