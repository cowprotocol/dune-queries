-- This query computes some simple cost coverage statistics
-- for a week. Although time range can be specified by the user
-- this query is aligned with the weekly payouts so its results
-- are useful when start/end date aligns with an accounting period 
-- Parameters:
--  {{start_time}}: the start date of an accounting week
--  {{end_time}}: the end date of an accounting week
--  {{blockchain}}: network to run the analysis on
with
wrapped_native_token as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
            when 'gnosis' then 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d -- WXDAI
            when 'arbitrum' then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- WETH
            when 'base' then 0x4200000000000000000000000000000000000006 -- WETH
            when 'avalanche_c' then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- WAVAX
            when 'polygon' then 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 -- WPOL
            when 'lens' then 0x6bdc36e20d267ff0dd6097799f82e78907105e2f -- WGHO
            when 'bnb' then 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c -- WBNB
        end as native_token_address
),

cow_token_address as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
            when 'gnosis' then 0x177127622c4a00f3d409b75571e12cb3c8973d3c
            when 'arbitrum' then 0xcb8b5cd20bdcaea9a010ac1f8d835824f5c87a04
            when 'base' then 0xc694a91e6b071bf030a18bd3053a7fe09b6dae69
        end as cow_address
),

rewards_safe as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 0xa03be496e67ec29bc62f01a428683d7f9c204930
            when 'gnosis' then 0xa03be496e67ec29bc62f01a428683d7f9c204930
            when 'arbitrum' then 0x66331f0b9cb30d38779c786bda5a3d57d12fba50
            when 'base' then 0xa03be496e67ec29bc62f01a428683d7f9c204930
            when 'avalanche_c' then 0xa03be496e67ec29bc62f01a428683d7f9c204930
            when 'polygon' then 0x66331f0b9cb30d38779c786bda5a3d57d12fba50
            when 'lens' then 0x798bb2d0ac591e34a4068e447782de05c27ed160
            when 'bnb' then 0xa03be496e67ec29bc62f01a428683d7f9c204930
        end as rewards_safe_address
),

purchased_native_token as (
    select
        block_time,
        sum(units_bought) as fees
    from cow_protocol_{{blockchain}}.trades
    where
        (
            buy_token_address = (select native_token_address from wrapped_native_token)
            or buy_token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        )
        and trader in (0x84e5c8518c248de590d5302fd7c32d2ae6b0123c, 0x9008d19f58aabd9ed0d60971565aa8510560ab41)
        and receiver = (select rewards_safe_address from rewards_safe)
        and block_date between (timestamp '{{start_time}}' + interval '12' hour) and (timestamp '{{end_time}}' + interval '12' hour)
        -- fee withdrawals are currently executed daily at midnight UTC. However, the fee withdrawal at the end of an accounting period
        -- should not be included in the next accounting period, this is why we add a shift of 12 hours, in order to ensure that the
        -- last withdrawal for an accounting period is indeed mapped to that accounting period and not to the one that follows
    group by block_time
),

-- Native token Transfer from settlement contract to rewards safe
transferred_native_token as (
    select
        evt_block_time as block_time,
        sum(value / 1e18) as fees
    from erc20_{{blockchain}}.evt_transfer
    where
        "from" = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
        and to = (select rewards_safe_address from rewards_safe)
        and contract_address = (select native_token_address from wrapped_native_token)
        and evt_tx_hash not in (select tx_hash from cow_protocol_{{blockchain}}.trades where block_date between (timestamp '{{start_time}}' + interval '12' hour) and (timestamp '{{end_time}}' + interval '12' hour))
        and evt_block_time between (timestamp '{{start_time}}' + interval '12' hour) and (timestamp '{{end_time}}' + interval '12' hour)
    group by evt_block_time
),

fees_native_token as (
    select
        max(block_time) as latest_withdrawal_time,
        sum(fees) as fees
    from (
        select
            block_time,
            fees
        from purchased_native_token
        union all
        select
            block_time,
            fees
        from transferred_native_token
    )
),

outgoing_native_token as (
    select sum(value / 1e18) as native_token
    from {{blockchain}}.traces
    where
        "from" = (select rewards_safe_address from rewards_safe)
        and success = true
        and (lower(call_type) not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
        and block_time between (timestamp '{{start_time}}' + interval '2' day) and (timestamp '{{end_time}}' + interval '2' day)
),

outgoing_cow as (
    select sum(value) / 1e18 as cow
    from erc20_{{blockchain}}.evt_transfer
    where
        contract_address = (select cow_address from cow_token_address)
        and "from" = (select rewards_safe_address from rewards_safe)
        and evt_block_time between (timestamp '{{start_time}}' + interval '2' day) and (timestamp '{{end_time}}' + interval '2' day)
),

conversion_prices as (
    select
        (
            select avg(price) from prices.usd
            where
                blockchain = '{{blockchain}}'
                and contract_address = (select cow_address from cow_token_address)
                and date(minute) = cast('{{end_time}}' as timestamp) - interval '1' day
        ) as cow_price,
        (
            select avg(price) from prices.usd
            where
                blockchain = '{{blockchain}}'
                and contract_address = (select native_token_address from wrapped_native_token)
                and date(minute) = cast('{{end_time}}' as timestamp) - interval '1' day
        ) as native_token_price
),

protocol_fees_collected as (
    select sum(protocol_fee * protocol_fee_native_price / pow(10, 18) - coalesce(case when partner_fee_recipient is not null then partner_fee * protocol_fee_native_price / pow(10, 18) end, 0)) as protocol_fee_in_native_token --noqa: RF01
    from "query_4364122(blockchain='{{blockchain}}')" as r
    inner join {{blockchain}}.blocks as b on number = block_number
    where
        b.time between timestamp '{{start_time}}' and timestamp '{{end_time}}'
        and r.order_uid not in (select order_uid from query_3639473)
),

results as (
    select
        (select native_token from outgoing_native_token) as outgoing_native_token,
        (select cow from outgoing_cow) as outgoing_cow,
        (select native_token from outgoing_native_token) + (select cow from outgoing_cow) * (select cow_price / native_token_price from conversion_prices) as total_outgoing_native_token,
        (select fees from fees_native_token) as fees_native_token,
        (select latest_withdrawal_time from fees_native_token) as last_withdrawal,
        (select protocol_fee_in_native_token from protocol_fees_collected) as protocol_fee_in_native_token
)

select
    *,
    -- does not include outgoing cow.
    fees_native_token / outgoing_native_token as cost_coverage,
    -- converts outgoing cow and adds to outgoing native token.
    fees_native_token / total_outgoing_native_token as effective_cost_coverage,
    fees_native_token - outgoing_native_token as profit,
    fees_native_token - total_outgoing_native_token as effective_profit
from results
