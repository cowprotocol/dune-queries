-- Computes the TVL, lp token total supply and lp token price of a curve pool over time
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run
--  {{blockchain}} - chain for which the query is

-- Given that we might not have records every day in the source data (e.g. not every day the lp supply may change), 
-- but still want to visualize development on a per day basis,  we create an auxiliary table with one record per 
-- day between `start` and `now`
with date_range as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '{{start}}'),
            date(now())
        )) t (day) --noqa: AL01
),

-- Finds the curve pool address given tokens specified in query parameters (regardless of order) 
-- The lp token address is not necessarily the address of the vault 
curve_pool as (
    select
        pool_address as address,
        token_address as lp_address
    from curvefi_{{blockchain}}.view_pools
    where
        (
            (coin0 = {{token_a}} and coin1 = {{token_b}})
            or (coin1 = {{token_a}} and coin1 = {{token_b}})
        )
        and (coin2 = 0x0000000000000000000000000000000000000000 or coin2 is null)
),

-- per day lp token total supply changes of the CoW AMM pool by looking at burn/mint events
lp_balance_delta as (
    select
        date(evt_block_time) as "day",
        sum(case when "from" = 0x0000000000000000000000000000000000000000 then value else -value end) as lp_supply
    from erc20_{{blockchain}}.evt_transfer
    where
        contract_address in (select lp_address from curve_pool)
        and ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
    group by 1
),

lp_total_supply_incomplete as (
    select
        day,
        lp_supply,
        sum(lp_supply) over (order by day) as total_lp
    from lp_balance_delta
),

-- performance optimisation: reduce the total range of the join below to the last value before the start period
lp_total_supply_start as (
    select max(day) as "start"
    from lp_total_supply_incomplete
    where day <= date(timestamp '{{start}}')
),

-- lp token total supply without date gaps
lp_total_supply as (
    select
        day,
        total_lp
    from (
        -- join full date range with potentially incomplete data. This results in many rows per day (all total supplies on or before that day)
        -- rank() is then used to order join candidates by recency (rank = 1 is the latest lp supply)
        select
            date_range.day,
            total_lp,
            rank() over (partition by (date_range.day) order by lp.day desc) as latest
        from date_range
        inner join lp_total_supply_incomplete as lp
            on date_range.day >= lp.day
            -- performance optimisation: this assumes one week prior to start there was at least one lp supply change event
            and lp.day >= (select start from lp_total_supply_start)
    )
    where latest = 1
),

-- Computes the token balance changes of the relevant token per transaction
reserves_delta as (
    select
        evt_block_time,
        p.address as pool,
        {{token_a}} as token1,
        {{token_b}} as token2,
        evt_block_number,
        evt_tx_hash as tx_hash,
        1 as cat,
        case
            when {{token_b}} = t.contract_address then 0
            when "from" = p.address then -value
            else value
        end as amount1,
        case
            when {{token_a}} = t.contract_address then 0
            when "from" = p.address then -value
            else value
        end as amount2
    from erc20_{{blockchain}}.evt_transfer as t
    inner join curve_pool as p
        on
            t."from" = p.address
            or t.to = p.address
    where contract_address in ({{token_a}}, {{token_b}})
    --WETH minting/burning
    union all
    select
        block_time as evt_block_time,
        (select address from curve_pool) as pool,
        {{token_a}} as token1,
        {{token_b}} as token2,
        block_number as evt_block_number,
        tx_hash,
        2 as cat,
        case
            when not ({{token_a}} = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2) then 0
            when topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c then varbinary_to_uint256(data) --deposit
            else -varbinary_to_uint256(data)
        end as amount1,
        case
            when not ({{token_b}} = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2) then 0
            when topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c then varbinary_to_uint256(data) --deposit
            else -varbinary_to_uint256(data)
        end as amount2
    from ethereum.logs
    where
        (
            topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c --deposit
            or topic0 = 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65 --withdrawal
        )
        and contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 --WETH
        and varbinary_to_uint256(topic1) = varbinary_to_uint256((select address from curve_pool))
    --ETH transfers
    union all
    select
        block_time as evt_block_time,
        (select address from curve_pool) as pool,
        {{token_a}} as token1,
        {{token_b}} as token2,
        block_number as evt_block_number,
        tx_hash,
        3 as cat,
        case
            when not ({{token_a}} = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2) then 0
            when to = 0x0E9B5B092caD6F1c5E6bc7f89Ffe1abb5c95F1C2 then value
            else -value
        end as amount1,
        case
            when not ({{token_b}} = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2) then 0
            when to = 0x0E9B5B092caD6F1c5E6bc7f89Ffe1abb5c95F1C2 then value
            else -value
        end as amount2
    from ethereum.traces
    where
        (
            "from" = (select address from curve_pool)
            or to = (select address from curve_pool)
        )
        and (lower(call_type) not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
        and success = true
),

reserves_delta_by_block as (
    select
        evt_block_time,
        pool,
        token1,
        token2,
        evt_block_number,
        max(tx_hash) as evt_tx_hash,
        sum(amount1) as amount1,
        sum(amount2) as amount2
    from reserves_delta
    group by 1, 2, 3, 4, 5
),

-- sums token balance changes to get total balances of relevant tokens per transaction
balances_by_tx as (
    select
        evt_block_time as block_time,
        evt_block_number,
        pool,
        token1,
        token2,
        amount1,
        amount2,
        sum(amount1) over (order by (evt_block_number)) as balance1,
        sum(amount2) over (order by (evt_block_number)) as balance2
    from reserves_delta_by_block
    -- performance optimisation: this assumes one week prior to start there was at least one tvl change event
    --where evt_block_time >= timestamp '{{start}}' - interval '7' day
),

tvl as (
    select
        tvl_complete.day,
        balance1 * p1.price + balance2 * p2.price as tvl
    from (
        -- join full date range with potentially incomplete data. This results in many rows per day (all pool balances on or before that day)
        -- rank() is then used to order join candidates by recency (rank = 1 is the latest pool balances)
        select
            date_range.day,
            token1,
            balance1,
            token2,
            balance2,
            rank() over (partition by (date_range.day) order by b.block_time desc) as latest
        from date_range
        inner join balances_by_tx as b
            on date_range.day >= date(b.block_time)
    ) as tvl_complete
    inner join prices.day as p1
        on
            tvl_complete.day = p1.timestamp
            and p1.contract_address = token1
            and p1.blockchain = '{{blockchain}}'
    inner join prices.day as p2
        on
            tvl_complete.day = p2.timestamp
            and p2.contract_address = token2
            and p2.blockchain = '{{blockchain}}'
    where latest = 1
),

-- With this we can plot the lp token price (tvl/lp total supply) over time
final as (
    select
        tvl.day,
        tvl,
        total_lp,
        tvl / total_lp as lp_token_price
    from tvl
    inner join lp_total_supply as lp
        on tvl.day = lp.day
)

-- Compute current value of initial investment together with other relevant output columns
select
    day,
    tvl,
    total_lp,
    lp_token_price,
    (
        -- Assess initial investment in lp tokens
        select 10000 / lp_token_price as investment
        from final
        where day = timestamp '{{start}}'
    ) * lp_token_price as current_value_of_investment
from final
where day >= timestamp '{{start}}'
order by 1 desc
