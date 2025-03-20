-- This query computes the relative surplus increase of a CoW AMM compared to its benchmarks.
-- Parameters:
--   cow_amm: the address of the pool
--   start: the start date

with cow_amm as (
    select
        created_at,
        contract_address,
        token0,
        token1,
        cast(weight0 as double) as weight0,
        cast(weight1 as double) as weight1,
        blockchain
    from dune.cowprotocol.result_amm_lp_infos
    where contract_address = {{cow_amm}}
),

-- gets the competitors to be comparing to CoW AMM.
-- We assume the table has one competitor with the same token pair per project.
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

-- With Xt and Yt being the reserves at time t, wx and wy the weights (wx+wy = 1) and LPt the lp reserves at time t
-- we define the curve per LP token : Ct = Xt**wx * Yt**wy / LPt
-- Then the relative surplus between t and t+1 is delta Surplus = C(t+1)/C(t) - 1. 
-- Obviously, the surplus is positive as it can only increase during a trade, and a deposit/withdrawal keeps Ct constant.
-- This formula takes into account the compounding effect of surplus over many transactions
-- The intuition is we compare the current curve to the same pool if it had been trading strictly on the curve (no surplus) and ended with the same marginal price Xt/Yt

-- weights are integers and sum to 100. They must be converted to double to avoid integer division
select
    cow.day,
    power(cow.reserve0, cast(cow.weight0 as double) / 100) * power(cow.reserve1, cast(cow.weight1 as double) / 100) / cow.lp_reserve
    / lag(power(cow.reserve0, cast(cow.weight0 as double) / 100) * power(cow.reserve1, cast(cow.weight1 as double) / 100) / cow.lp_reserve) over (order by cow.day) - 1 as cow_relative_invariant_growth,
    power(uni.reserve0, cast(uni.weight0 as double) / 100) * power(uni.reserve1, cast(uni.weight1 as double) / 100) / uni.lp_reserve
    / lag(power(uni.reserve0, cast(uni.weight0 as double) / 100) * power(uni.reserve1, cast(uni.weight1 as double) / 100) / uni.lp_reserve) over (order by uni.day) - 1 as uni_relative_invariant_growth,
    power(sushi.reserve0, cast(sushi.weight0 as double) / 100) * power(sushi.reserve1, cast(sushi.weight1 as double) / 100) / sushi.lp_reserve
    / lag(power(sushi.reserve0, cast(sushi.weight0 as double) / 100) * power(sushi.reserve1, cast(sushi.weight1 as double) / 100) / sushi.lp_reserve) over (order by sushi.day) - 1 as sushi_relative_invariant_growth,
    power(pancake.reserve0, cast(pancake.weight0 as double) / 100) * power(pancake.reserve1, cast(pancake.weight1 as double) / 100) / pancake.lp_reserve
    / lag(power(pancake.reserve0, cast(pancake.weight0 as double) / 100) * power(pancake.reserve1, cast(pancake.weight1 as double) / 100) / pancake.lp_reserve) over (order by pancake.day) - 1 as pancake_relative_invariant_growth
from dune.cowprotocol.result_amm_lp_infos as cow
--filter before joining to avoid removing everything if one benchmark does not exist
left join (select * from dune.cowprotocol.result_amm_lp_infos where contract_address in (select contract_address from competitors where project = 'uniswapv2')) as uni --noqa: ST05
    on cow.day = uni.day
left join (select * from dune.cowprotocol.result_amm_lp_infos where contract_address in (select contract_address from competitors where project = 'sushiswapv2')) as sushi --noqa: ST05
    on cow.day = sushi.day
left join (select * from dune.cowprotocol.result_amm_lp_infos where contract_address in (select contract_address from competitors where project = 'pancakeswap')) as pancake --noqa: ST05
    on cow.day = pancake.day
where
    cow.contract_address in (select contract_address from competitors where project = 'cow_amm')
    -- we start whenever all benchmarks can start
    and cow.day >= (select max(start) from competitors)
