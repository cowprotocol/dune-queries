-- Computes the TVL for every Pancakeswap pool
-- Then returns the top {{number_of_pools}} pools by TVL
-- Input: blockchain, number_of_pools to return

with 
-- finds the pools which have been active since 2024-10-01
data as (
select 
    p.contract_address as pool_address,
    call_block_time as time,
    output__reserve0 as balance0,
    output__reserve1 as balance1,
    rank() over (partition by p.contract_address order by p.call_block_time desc) latest
from pancakeswap_v2_{{blockchain}}.PancakePair_call_getReserves p 
where date_trunc('day',call_block_time) >= date_trunc('day', cast('2024-10-01' as date))),

--Gets the token0 and token1 addresses for each pool
t0 as (select contract_address, max(output_0) as token0 
    from pancakeswap_v2_{{blockchain}}.PancakePair_call_token0
    group by contract_address),
    
t1 as (select contract_address, max(output_0) as token1
    from pancakeswap_v2_{{blockchain}}.PancakePair_call_token1
    group by contract_address),

--computes the tvl for each pool
-- for each pool we could get multiple balance values if the function was called multiple times in a same block
-- we arbitrarily choose the maximum value for each pool
recent_tvl as(
select pool_address,
token0, max(balance0) as balance0,
token1, max(balance1) as balance1,
max(least(balance0,balance1)* greatest(p0.price/pow(10, p0.decimals),p1.price/pow(10, p1.decimals)) +
 greatest(balance0,balance1)* least(p0.price/pow(10, p0.decimals),p1.price/pow(10, p1.decimals))) as tvl
from data
join t0
    on pool_address = t0.contract_address
join t1
    on pool_address = t1.contract_address
join prices.usd_latest as p0
    on token0 = p0.contract_address
join prices.usd_latest as p1
    on token1 = p1.contract_address
where latest = 1
group by 1,2,4)

select * from recent_tvl
order by tvl desc
limit {{number_of_pools}}