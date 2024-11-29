-- Computes the swap fee per $100 tvl for each day (aka its invariant growth) for a Balancer pool.
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run
--  {{blockchain}} - chain for which the query is running

-- Limit the date range
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
        and blockchain = '{{blockchain}}'
        and pool_type <> 'balancer_cowswap_amm'
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
    order by tvl desc
    limit 1
),

-- compute $ tvl using the same price feed we use for other reference pools 
-- (as the price feed that balance uses seems inaccurate)
tvl as (
    select
        l.day,
        l.pool_address,
        l.blockchain,
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
    group by l.day, l.pool_address, l.blockchain
)

select
    dr.day,
    swap_amount_usd as volume,
    fee_amount_usd as absolute_invariant_growth,
    tvl,
    fee_amount_usd / tvl as pct_invariant_growth
from date_range as dr
left join tvl
    on dr.day = tvl.day
left join balancer.pools_metrics_daily as bal
    on
        dr.day = bal.block_date
        and project_contract_address = tvl.pool_address
        and tvl.blockchain = bal.blockchain
order by dr.day desc
