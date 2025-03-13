with cow_amm_pools as (
    select
        *,
        reserve0 * price0 * power(10, -decimals0) + reserve1 * price1 * power(10, -decimals1) as current_pool_value,
        lag(price0) over (partition by contract_address order by day asc) as previous_price0,
        lag(price1) over (partition by contract_address order by day asc) as previous_price1
    from "query_4420687(blockchain = 'ethereum', start = '2025-01-01', end = '2100-01-01')"
    where lp_reserve > 0
),

uni_style_pools as (
    select
        uni.*,
        uni.reserve0 * cow.price0 * power(10, -cow.decimals0) + uni.reserve1 * cow.price1 * power(10, -cow.decimals1) as current_pool_value,
        cow.contract_address as cow_amm_contract_address
    from "query_4420675(blockchain = 'ethereum', start = '2025-01-01', end = '2100-01-01')" as uni
    inner join cow_amm_pools as cow
        on cow.token0 = uni.token0
        and cow.token1 = uni.token1
        and cow.day = uni.day
        and cow.weight0 = 50
        and cow.weight1 = 50
    where uni.lp_reserve > 0
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
    10000 * c.current_pool_value / c.lp_reserve * first_value(c.lp_reserve / c.pool_value) over (
        partition by c.contract_address order by c.day asc
    ) as cow_amm_investment_value,

    coalesce(exp(sum(ln((previous_price0/price0 + previous_price1/price1) / 2)) over (order by day asc)), 1) * 10000 as rebalancing,

    10000 * weight0 / 100 * price1 / first_value(price1) over (partition by contract_address order by day asc) +
        10000 * weight1 / 100 * price0 / first_value(price0) over (partition by contract_address order by day asc) as hodl,
    
    uni.contract_address as uni_contract_address,
    10000 * uni.current_pool_value / uni.lp_reserve * first_value(uni.lp_reserve / uni.pool_value) over (
        partition by uni.contract_address order by uni.day asc
    ) as uniswap_investment_value,

    sushi.contract_address as sushi_contract_address,
    10000 * sushi.current_pool_value / sushi.lp_reserve * first_value(sushi.lp_reserve / sushi.pool_value) over (
        partition by sushi.contract_address order by sushi.day asc
    ) as sushiwap_investment_value,

    pancake.contract_address as pancake_contract_address,
    10000 * pancake.current_pool_value / pancake.lp_reserve * first_value(pancake.lp_reserve / pancake.pool_value) over (
        partition by pancake.contract_address order by pancake.day asc
    ) as panackeswap_investment_value

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
