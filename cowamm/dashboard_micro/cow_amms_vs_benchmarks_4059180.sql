-- Compute the evolution of performance for $10k invested in different strategies
-- Parameters:
--   cow_amm: the address of the pool
--   start: the start date

with cow_amm as (
    select
        created_at,
        contract_address,
        token0,
        token1,
        weight0,
        weight1,
        blockchain
    from dune.cowprotocol.result_amm_lp_infos
    where contract_address = {{cow_amm}}
),

--we assume that in the table there is one competitor with the same token pair per project
competitors as (
    select
        i.contract_address,
        i.project,
        i.created_at,
        greatest(min(i.day), timestamp '{{start}}') as "start"
    from dune.cowprotocol.result_amm_lp_infos as i --noqa: ST09
    inner join cow_amm as c
        on
            (c.token0 = i.token0 and c.token1 = i.token1 and c.weight0 = i.weight0 and c.weight1 = i.weight1)
            or (c.token1 = i.token0 and c.token0 = i.token1 and c.weight1 = i.weight0 and c.weight0 = i.weight1)
    where
        (project != 'cow_amm' or i.contract_address = {{cow_amm}})
        and reserve0 > 0
        and reserve1 > 0
        and lp_reserve > 0
    group by 1, 2, 3
),

-- precaluclation for the rebalancing
prices as (
    select
        day,
        price0 / lag(price0) over (order by day asc) as p0,
        price1 / lag(price1) over (order by day asc) as p1
    from dune.cowprotocol.result_amm_lp_infos
    where
        contract_address = {{cow_amm}}
        and day >= (select max(start) from competitors)
)

select
    cow.day,
    10000 * (cow.value0 + cow.value1) / cow.lp_reserve / first_value((cow.value0 + cow.value1) / cow.lp_reserve) over (order by cow.day asc) as cow_10k,
    cow.value0 + cow.value1 as cow_tvl,
    10000 * (uni.value0 + uni.value1) / uni.lp_reserve / first_value((uni.value0 + uni.value1) / uni.lp_reserve) over (order by uni.day asc) as uni_10k,
    uni.value0 + uni.value1 as uni_tvl,
    10000 * (sushi.value0 + sushi.value1) / sushi.lp_reserve / first_value((sushi.value0 + sushi.value1) / sushi.lp_reserve) over (order by sushi.day asc) as sushi_10k,
    sushi.value0 + sushi.value1 as sushi_tvl,
    10000 * (pancake.value0 + pancake.value1) / pancake.lp_reserve / first_value((pancake.value0 + pancake.value1) / pancake.lp_reserve) over (order by pancake.day asc) as pancake_10k,
    pancake.value0 + pancake.value1 as pancake_tvl,
    5000 * cow.price0 / first_value(cow.price0) over (order by cow.day asc) + 5000 * cow.price1 / first_value(cow.price1) over (order by cow.day asc) as hodl_10k,
    -- SQL doesn't support PRODUCT() over (...), but luckily "the sum of logarithms" is equal to "logarithm of the product",
    -- coalesce to factor 1 on first day
    coalesce(exp(sum(ln((prices.p0 + prices.p1) / 2)) over (order by cow.day asc)), 1) * 10000 as rebalance_10k

from dune.cowprotocol.result_amm_lp_infos as cow
left join (select * from dune.cowprotocol.result_amm_lp_infos where contract_address in (select contract_address from competitors where project = 'uniswapv2')) as uni --noqa: ST05
    on cow.day = uni.day
left join (select * from dune.cowprotocol.result_amm_lp_infos where contract_address in (select contract_address from competitors where project = 'sushiswapv2')) as sushi --noqa: ST05
    on cow.day = sushi.day
left join (select * from dune.cowprotocol.result_amm_lp_infos where contract_address in (select contract_address from competitors where project = 'pancakeswap')) as pancake --noqa: ST05
    on cow.day = pancake.day
left join prices
    on cow.day = prices.day
where
    cow.contract_address in (select contract_address from competitors where project = 'cow_amm')
    -- we start whenever all benchmarks can start
    and cow.day >= (select max(start) from competitors)
