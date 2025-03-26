-- Shows a summary of all CoW AMMs accross chains

with recent as (
    select
        blockchain,
        contract_address,
        created_at,
        symbol0,
        symbol1,
        token0,
        token1,
        reserve0,
        reserve1,
        lp_reserve,
        value0 + value1 as tvl,
        cast(weight0 as double) / 100 as weight0,
        cast(weight1 as double) / 100 as weight1,
        concat(
            '<a href="https://dune.com/cowprotocol/cow-amm-micro-v2?blockchain=', blockchain, '&cow_amm=', cast(contract_address as varchar),
            '" target="_blank">', cast(contract_address as varchar), '</a>'
        ) as cow_amm_address,
        rank() over (partition by contract_address order by day desc) as latest
    from dune.cowprotocol.result_amm_lp_infos
    where
        project = 'cow_amm'
)

select --noqa: ST06
    r.blockchain,
    r.cow_amm_address,
    r.created_at,
    r.tvl,
    r.symbol0,
    r.symbol1,
    -- Compute surplus based on the curve's value
    round(
        power(
            power(r.reserve0, r.weight0) * power(r.reserve1, r.weight1) / r.lp_reserve
            / (power(r1.reserve0, r1.weight0) * power(r1.reserve1, r1.weight1) / r1.lp_reserve),
            365
        ) - 1, 4
    ) as "1d APY",
    round(
        power(
            power(r.reserve0, r.weight0) * power(r.reserve1, r.weight1) / r.lp_reserve
            / (power(r2.reserve0, r2.weight0) * power(r2.reserve1, r2.weight1) / r2.lp_reserve),
            365 / 7
        ) - 1, 4
    ) as "7d APY",
    round(
        power(
            power(r.reserve0, r.weight0) * power(r.reserve1, r.weight1) / r.lp_reserve
            / (power(r3.reserve0, r3.weight0) * power(r3.reserve1, r3.weight1) / r3.lp_reserve),
            365 / 30
        ) - 1, 4
    ) as "30d APY",
    r.token0,
    r.token1
from recent as r
left join recent as r1
    on
        r.contract_address = r1.contract_address
        and r1.latest = 2 -- 1 day
left join recent as r2
    on
        r.contract_address = r2.contract_address
        and r2.latest = 8 -- 1 week
left join recent as r3
    on
        r.contract_address = r3.contract_address
        and r3.latest = 31 -- 30 day
where
    -- get the value today
    r.latest = 1
    --remove empty pools
    and r.lp_reserve > 0
    and r.reserve0 > 0
    and r.reserve1 > 0
order by tvl desc
