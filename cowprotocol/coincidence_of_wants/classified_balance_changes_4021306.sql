-- This query is part of a base query for computing CoWs
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- The query tags all transfers to and from the settlement contract as one of
-- - user_in: amounts sent by users, tokens flowing into the settlement contract
-- - user_out: amount sent to users, tokens flowing out of the settlement contract
-- - amm_in: tokens flowing into the settlement contract but coming from users
-- - amm_out:tokens flowing out of the settlement contract but not towards users
-- - slippage_in: final imbalance of the settlement contract if that imbalance is positive
-- - slippage_out: final imbalance of the settlement contract of that imbalance is negative
--
-- The classification into user transfers and amm transfers depends on a clear separation of addresses into
-- trader addresses and amm addresses. If an address is used in a trade, all interactions with that address
-- are classified as user transfers, even in other transactions.
--
-- The common edge case of the settlement contract acting as a trader is implicitly handled sa follows:
-- The settlement contract will appear as a trader. There will be a transfer from the settlement contract to itself.
-- The corresponding balance change is accounted for as 'user_in'.
-- This behavior will be wrong when an order is created with receiver set to the settlement contract.

with filtered_trades as (
    select
        *
    from cow_protocol_{{blockchain}}.trades
    where block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp)
),
traders as (
    select
        trader as sender,
        receiver
    from filtered_trades
),
balance_changes as (
    select
        *
    from "query_4021257(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),
-- classify balance changes
balance_changes_with_types as (
    select
        block_time,
        tx_hash,
        token_address,
        amount,
        case
            -- user in
            when (sender in (select sender from traders) -- transfer coming from a trader
                or sender = 0x40a50cf069e992aa4536211b23f286ef88752187) -- transfer coming from ETH flow
                and receiver = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 -- transfer going to the settlement contract
            then 'user_in'
            -- user out
            when receiver in (select receiver from traders) -- transfer going to a trader
                and sender = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 -- transfer coming from the settlement contract
            then 'user_out'
            -- amm in
            when receiver = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
            then 'amm_in'
            -- amm out
            when sender = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
            then 'amm_out'
        end as transfer_type
    from balance_changes
),
slippage as (
    select
        block_time,
        tx_hash,
        token_address,
        case when amount >= 0 then cast(amount as uint256) else cast(-amount as uint256) end as amount,
        case when amount >= 0 then 'slippage_in' else 'slippage_out' end as transfer_type
    from "query_4021644(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
)

select * from balance_changes_with_types
union all
select * from slippage
