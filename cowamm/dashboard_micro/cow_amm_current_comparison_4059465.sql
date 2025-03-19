-- Current outperformance of CoW AMM vs Uniswap and Sushiswap and TVL
-- Parameters:
-- cow_amm: CoW AMM address
-- start: Start date for the query

with comparison as (
    select *
    from "query_4844142(cow_amm = '{{cow_amm}}', start = '{{start}}')"
)

select
    c.cow_tvl,
    c.uni_tvl,
    c.sushi_tvl,
    c.pancake_tvl,
    ((c.cow_10k / c.uni_10k) - 1) as over_uni_return,
    ((c.cow_10k / c.sushi_10k) - 1) as over_sushi_return,
    ((c.cow_10k / c.pancake_10k) - 1) as over_pancake_return,
    ((c.cow_10k / c.rebalance_10k) - 1) as over_reb_return,
    -- 7 day growth
    ((c.cow_10k / c7.cow_10k / (c.uni_10k / c7.uni_10k)) - 1) as over_uni_return_1w,
    ((c.cow_10k / c7.cow_10k / (c.sushi_10k / c7.sushi_10k)) - 1) as over_sushi_return_1w,
    ((c.cow_10k / c7.cow_10k / (c.pancake_10k / c7.pancake_10k)) - 1) as over_pancake_return_1w,
    ((c.cow_10k / c7.cow_10k / (c.rebalance_10k / c7.rebalance_10k)) - 1) as over_reb_return_1w
from comparison as c
inner join comparison as c7
    on c.day = c7.day + interval '7' day
order by c.day desc
limit 1
