-- Computes the TVL, lp token total supply and lp token price of a uniswap pool over time
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run
--  {{blockchain}} - chain for which the query is running

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

-- prefilter only relevant pools
relevant_liquidity as (
    select *
    from balancer.liquidity
    where
        token_address in ({{token_a}}, {{token_b}})
        and pool_type <> 'balancer_cowswap_amm'
        and blockchain = '{{blockchain}}'
        and day = date(now())
),

-- get the pool with the largest TVL
pool as (
    select
        l1.pool_address,
        l1.blockchain,
        l1.pool_liquidity_usd + l2.pool_liquidity_usd as tvl
    from relevant_liquidity as l1
    inner join relevant_liquidity as l2
        on
            l1.pool_address = l2.pool_address
            and l1.token_address = {{token_a}}
            and l2.token_address = {{token_b}}
    order by 3 desc
    limit 1
),

-- per day lp token total supply changes of the uniswap pool by looking at burn/mint events
lp_supply_delta as (
    select
        date(evt_block_time) as "day",
        sum(case when "from" = 0x0000000000000000000000000000000000000000 then value else -value end) as delta
    from erc20_{{blockchain}}.evt_transfer
    where
        contract_address = (select pool_address from pool)
        and ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
    group by 1
),

-- per day lp token total supply by summing up all deltas (may have missing records for some days)
lp_total_supply_incomplete as (
    select
        day,
        sum(delta) over (order by day) as total_lp
    from lp_supply_delta
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
            on
                date_range.day >= lp.day
                and lp.day >= (select start from lp_total_supply_start)
    )
    where latest = 1
),

-- Get tvl by multiplying day end reserves with their token's closing price
tvl as (
    select
        l.day,
        sum(token_balance * price_close) as tvl
    from balancer.liquidity as l
    inner join pool
        on
            l.pool_address = pool.pool_address
            and l.blockchain = pool.blockchain
    left join prices.minute_daily as p1
        on
            l.blockchain = p1.blockchain
            and l.token_address = p1.contract_address
            and l.day = p1.day
    group by 1
),

lp_token_price as (
    select
        dr.day,
        tvl,
        total_lp,
        tvl / total_lp as lp_token_price
    from date_range as dr
    left join tvl
        on dr.day = tvl.day
    left join lp_total_supply as lp
        on dr.day = lp.day
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
        from lp_token_price
        where day = timestamp '{{start}}'
    ) * lp_token_price as current_value_of_investment
from lp_token_price
order by 1 desc
