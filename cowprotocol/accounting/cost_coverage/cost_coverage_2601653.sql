-- This query computes some simple cost coverage statistics
-- for a week. Although time range can be specified by the user
-- this query is aligned with the weekly payouts so its results
-- are useful when start/end date aligns with an accounting period 
-- Parameters:
-- {{start_time}}: the start date of an accounting week
-- {{end_time}}: the end date of an accounting week
with
purchased_eth as (
    select
        block_time,
        sum(units_bought) as fees
    from cow_protocol_ethereum.trades
    where
        buy_token_address in (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)
        and trader in (0x84e5c8518c248de590d5302fd7c32d2ae6b0123c, 0x9008D19f58AAbD9eD0D60971565AA8510560ab41)
        and receiver = 0xa03be496e67ec29bc62f01a428683d7f9c204930
        and block_date between (timestamp '{{start_time}}' + interval '12' hour) and (timestamp '{{end_time}}' + interval '12' hour)
        -- fee withdrawals are currently executed daily at midnight UTC. However, the fee withdrawal at the end of an accounting period
        -- should not be included in the next accounting period, this is why we add a shift of 12 hours, in order to ensure that the
        -- last withdrawal for an accounting period is indeed mapped to that accounting period and not to the one that follows
    group by block_time
),

-- WETH Transfer from settlement contract to rewards safe
transferred_eth as (
    select
        evt_block_time as block_time,
        sum(value / 1e18) as fees
    from erc20_ethereum.evt_Transfer
    where
        "from" = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
        and to = 0xa03be496e67ec29bc62f01a428683d7f9c204930
        and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        and evt_tx_hash not in (select tx_hash from cow_protocol_ethereum.trades where block_date between (timestamp '{{start_time}}' + interval '12' hour) and (timestamp '{{end_time}}' + interval '12' hour))
        and evt_block_time between (timestamp '{{start_time}}' + interval '12' hour) and (timestamp '{{end_time}}' + interval '12' hour)
    group by evt_block_time
),

fees_eth as (
    select
        max(block_time) as latest_withdrawal_time,
        sum(fees) as fees
    from (
        select
            block_time,
            fees
        from purchased_eth
        union all
        select
            block_time,
            fees
        from transferred_eth
    )
),

outgoing_eth as (
    select sum(value / 1e18) as eth
    from ethereum.traces
    where
        "from" = 0xa03be496e67ec29bc62f01a428683d7f9c204930
        and success = true
        and (lower(call_type) not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
        and block_time between (timestamp '{{start_time}}' + interval '2' day) and (timestamp '{{end_time}}' + interval '2' day)
),

outgoing_cow as (
    select sum(value) / 1e18 as cow
    from erc20_ethereum.evt_Transfer
    where
        contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
        and "from" = 0xa03be496e67ec29bc62f01a428683d7f9c204930
        and evt_block_time between (timestamp '{{start_time}}' + interval '2' day) and (timestamp '{{end_time}}' + interval '2' day)
),

conversion_prices as (
    select
        (
            select avg(price) from prices.usd
            where
                blockchain = 'ethereum'
                and contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
                and date(minute) = cast('{{end_time}}' as timestamp) - interval '1' day
        ) as cow_price,
        (
            select avg(price) from prices.usd
            where
                blockchain = 'ethereum'
                and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
                and date(minute) = cast('{{end_time}}' as timestamp) - interval '1' day
        ) as eth_price
),

protocol_fees_collected as (
    select sum(cast(cast(data.protocol_fee as varchar) as int256) * data.protocol_fee_native_price / pow(10, 18) - coalesce(case when data.partner_fee_recipient is not null then cast(data.partner_fee as int256) * data.protocol_fee_native_price / pow(10, 18) end, 0)) as protocol_fee_in_eth --noqa: RF01
    from cowswap.raw_order_rewards as r
    inner join ethereum.blocks as b on number = block_number
    where
        r.block_number > 19068880
        and data.protocol_fee_native_price > 0 --noqa: RF01
        and b.time between timestamp '{{start_time}}' and timestamp '{{end_time}}'
        and r.order_uid not in (select cast(order_uid as varchar) from query_3639473)
),

results as (
    select
        (select eth from outgoing_eth) as outgoing_eth,
        (select cow from outgoing_cow) as outgoing_cow,
        (select eth from outgoing_eth) + (select cow from outgoing_cow) * (select cow_price / eth_price from conversion_prices) as total_outgoing_eth,
        (select fees from fees_eth) as fees_eth,
        (select latest_withdrawal_time from fees_eth) as last_withdrawal,
        (select protocol_fee_in_eth from protocol_fees_collected) as protocol_fee_in_eth
)

select
    *,
    -- does not include outgoing cow.
    fees_eth / outgoing_eth as cost_coverage,
    -- converts outgoing cow and adds to outgoing eth.
    fees_eth / total_outgoing_eth as effective_cost_coverage,
    fees_eth - outgoing_eth as profit,
    fees_eth - total_outgoing_eth as effective_profit
from results
