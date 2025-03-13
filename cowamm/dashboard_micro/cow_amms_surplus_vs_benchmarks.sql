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
)

select
    cow.day,
    power(cow.reserve0, cow.weight0 / 100) * power(cow.reserve1, cow.weight1 / 100) / cow.lp_reserve
    / lag(power(cow.reserve0, cow.weight0 / 100) * power(cow.reserve1, cow.weight1 / 100) / cow.lp_reserve) over (order by cow.day) - 1 as cow_surplus,
    power(uni.reserve0, uni.weight0 / 100) * power(uni.reserve1, uni.weight1 / 100) / uni.lp_reserve
    / lag(power(uni.reserve0, uni.weight0 / 100) * power(uni.reserve1, uni.weight1 / 100) / uni.lp_reserve) over (order by uni.day) - 1 as uni_surplus,
    power(sushi.reserve0, sushi.weight0 / 100) * power(sushi.reserve1, sushi.weight1 / 100) / sushi.lp_reserve
    / lag(power(sushi.reserve0, sushi.weight0 / 100) * power(sushi.reserve1, sushi.weight1 / 100) / sushi.lp_reserve) over (order by sushi.day) - 1 as sushi_surplus,
    power(pancake.reserve0, pancake.weight0 / 100) * power(pancake.reserve1, pancake.weight1 / 100) / pancake.lp_reserve
    / lag(power(pancake.reserve0, pancake.weight0 / 100) * power(pancake.reserve1, pancake.weight1 / 100) / pancake.lp_reserve) over (order by pancake.day) - 1 as pancake_surplus
from dune.cowprotocol.result_amm_lp_infos as cow
left join dune.cowprotocol.result_amm_lp_infos as uni
    on cow.day = uni.day
left join dune.cowprotocol.result_amm_lp_infos as sushi
    on cow.day = sushi.day
left join dune.cowprotocol.result_amm_lp_infos as pancake
    on cow.day = pancake.day
where
    cow.contract_address in (select contract_address from competitors where project = 'cow_amm')
    and cow.day >= (select max(start) from competitors)
    and uni.contract_address in (select contract_address from competitors where project = 'uniswapv2')
    and sushi.contract_address in (select contract_address from competitors where project = 'sushiswapv2')
    and pancake.contract_address in (select contract_address from competitors where project = 'pancakeswapv2')
