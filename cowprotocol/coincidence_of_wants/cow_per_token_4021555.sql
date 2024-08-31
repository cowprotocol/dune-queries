-- This query computes Coincidence of Wants fractions on CoW Protocol for individual tokens
--
-- It uses transfer information from query 4021306.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- The cow metrics computed are
-- - naive_cow_potential: min(user_in / user_out, 1)
--   This quantity indicates what fraction of the amount bought of a token is sold by other users.
-- - naive_cow: max(min((user_in - amm_out - slippage_in) / user_out, 1), 0)
--   This quantity indicates what fraction of the amount bought is not traded via AMMs or internalizations (showing as slippage).
-- - naive_cow_averaged: max(((user_in + user_out) - (amm_in + amm_out) - (slippage_in + slippage_out)) / (user_in + user_out), 0)
--   This quantity indicates what fraction of the amount bought and sold is not traded via AMMs or internalizations (showing as slippage).
--
-- The query also returns aggregated amounts for user_in, user_out, amm_in, amm_out, slippage_in, slippage_out for all tokens.

with aggregate_transfers_with_types as (
    select
        block_time,
        tx_hash,
        token_address,
        sum(amount) as amount,
        transfer_type
    from "query_4021306(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
    group by block_time, tx_hash, token_address, transfer_type
),
transfers_per_token as (
    select
        block_time,
        tx_hash,
        token_address,
        sum(case when transfer_type = 'user_in' then cast(amount as int256) else 0 end) as user_in, -- sum is selecting the only entry
        sum(case when transfer_type = 'user_out' then cast(amount as int256) else 0 end) as user_out,
        sum(case when transfer_type = 'amm_in' then cast(amount as int256) else 0 end) as amm_in,
        sum(case when transfer_type = 'amm_out' then cast(amount as int256) else 0 end) as amm_out,
        sum(case when transfer_type = 'slippage_in' then cast(amount as int256) else 0 end) as slippage_in,
        sum(case when transfer_type = 'slippage_out' then cast(amount as int256) else 0 end) as slippage_out
    from aggregate_transfers_with_types
    group by block_time, tx_hash, token_address
)

select
    *,
    case when user_out > 0 then least(1.0 * user_in / user_out, 1.0) else null end as naive_cow_potential,
    case when user_out > 0 then greatest(least(1.0 * (user_in - amm_out - slippage_in) / user_out, 1.0), 0.0) else null end as naive_cow,
    case when user_in + user_out > 0 then greatest(1.0 * ((user_in + user_out) - (amm_in + amm_out) - (slippage_in + slippage_out)) / (user_in + user_out), 0.0) else null end as naive_cow_averaged
from transfers_per_token
