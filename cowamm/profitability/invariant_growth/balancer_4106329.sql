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
)

select
    day,
    swap_amount_usd as volume,
    fee_amount_usd as absolute_invariant_growth,
    tvl_usd as tvl,
    fee_amount_usd / tvl_usd as pct_invariant_growth
from date_range as dr
cross join pool
left join balancer.pools_metrics_daily as bal
    on
        dr.day = bal.block_date
        and project_contract_address = pool_address
        and pool.blockchain = bal.blockchain
order by day desc
