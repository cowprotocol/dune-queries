with cow_amm_pools as (
    select
        *,
        reserve0 * price0 * power(10, -decimals0) + reserve1 * price1 * power(10, -decimals1) as current_pool_value,
        first_value(reserve0 * price0 * power(10, -decimals0) + reserve1 * price1 * power(10, -decimals1)) over (
            partition by contract_address
            order by case when lp_reserve > 0 then 0 else 1 end asc, day asc
        ) as initial_pool_value,
        first_value(lp_reserve) over (
            partition by contract_address
            order by case when lp_reserve > 0 then 0 else 1 end asc, day asc
        ) as initial_lp_reserve,
        lag(price0) over (partition by contract_address order by day asc) as previous_price0,
        lag(price1) over (partition by contract_address order by day asc) as previous_price1
    from "query_4420687(blockchain = 'ethereum', start = '2025-01-01', end = '2100-01-01')"
),

uni_style_pools as (
    select
        uni.*,
        uni.reserve0 * cow.price0 * power(10, -cow.decimals0) + uni.reserve1 * cow.price1 * power(10, -cow.decimals1) as current_pool_value,
        first_value(uni.reserve0 * cow.price0 * power(10, -cow.decimals0) + uni.reserve1 * cow.price1 * power(10, -cow.decimals1)) over (
            partition by uni.contract_address
            order by case when uni.lp_reserve > 0 then 0 else 1 end asc, uni.day asc
        ) as initial_pool_value,
        first_value(uni.lp_reserve) over (
            partition by uni.contract_address
            order by case when uni.lp_reserve > 0 then 0 else 1 end asc, uni.day asc
        ) as initial_lp_reserve,
        cow.contract_address as cow_amm_contract_address
    from "query_4420675(blockchain = 'ethereum', start = '2025-01-01', end = '2100-01-01')" as uni
    inner join cow_amm_pools as cow
        on cow.token0 = uni.token0
        and cow.token1 = uni.token1
        and cow.day = uni.day
        and cow.weight0 = 50
        and cow.weight1 = 50
),

rebalancing as (
    select
        contract_address,
        day,
        token0,
        token1,
        coalesce(exp(sum(ln((previous_price0/price0 + previous_price1/price1) / 2)) over (order by day asc)), 1) * 10000 as current_value_of_investment
    from cow_amm_pools
),

hodl as (
    select 
        contract_address,
        day,
        token0,
        token1,
        10000 * weight0 / 100 * price1 / first_value(price1) over (partition by contract_address order by day asc) +
        10000 * weight1 / 100 * price0 / first_value(price0) over (partition by contract_address order by day asc) as current_value_of_investment
    from cow_amm_pools
)

select
    c.contract_address as cow_amm_contract_address,
    c.day,
    c.token0,
    c.token1,
    weight0,
    weight1,
    c.price0,
    c.price1,
    c.current_pool_value / c.lp_reserve * c.initial_lp_reserve / c.initial_pool_value * 10000 as cow_amm_investment_value,
    uni.contract_address as uni_contract_address,
    uni.current_pool_value as uni_current_pool_value,
    uni.current_pool_value / uni.lp_reserve * uni.initial_lp_reserve / uni.initial_pool_value * 10000 as uni_investment_value,
    sushi.contract_address as sushi_contract_address,
    sushi.current_pool_value / sushi.lp_reserve * sushi.initial_lp_reserve / sushi.initial_pool_value * 10000 as sushi_investment_value,
    pancake.contract_address as pancake_contract_address,
    pancake.current_pool_value / pancake.lp_reserve * pancake.initial_lp_reserve / pancake.initial_pool_value * 10000 as pancake_investment_value
from cow_amm_pools as c
inner join rebalancing as r
    on c.contract_address = r.contract_address
    and c.day = r.day
inner join hodl as h
    on c.contract_address = h.contract_address
    and c.day = h.day
left join uni_style_pools as uni
    on c.contract_address = uni.cow_amm_contract_address
    and uni.project = 'uniswapv2'
left join uni_style_pools as sushi
    on c.contract_address = sushi.cow_amm_contract_address
    and sushi.project = 'sushiswapv2'
left join uni_style_pools as pancake
    on c.contract_address = pancake.cow_amm_contract_address
    and pancake.project = 'pancakeswapv2'
