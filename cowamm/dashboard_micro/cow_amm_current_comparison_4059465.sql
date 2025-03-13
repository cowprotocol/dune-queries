--Current outperformance of COW AMM vs Uniswap and Sushiswap and TVL
-- Parameters:
-- cow_amm: COW AMM address
-- start: Start date for the query

select 
    ((cow_10k/uni_10k)-1) as over_uni_return,
    ((cow_10k/sushi_10k)-1) as over_sushi_return,
    ((cow_10k/pancake_10k)-1) as over_pancake_return,
    ((cow_10k/rebalance_10k)-1) as over_reb_return,
    cow_tvl,
    uni_tvl,
    sushi_tvl,
    pancake_tvl
from "query_4059180(cow_amm = '{{cow_amm}}', start = '{{start}}')"
order by day desc
limit 1
