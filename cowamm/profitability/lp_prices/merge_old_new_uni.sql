--merge with old data


with old_last as (
    select
        contract_address,
        lp_reserve as last_lp_reserve,
        rank() over (partition by contract_address order by day desc) as latest
    from old
),

aggregate_transfer as (
    select
        block_day,
        new.contract_address,
        project,
        token0,
        token1,
        weight0,
        weight1,
        reserve0,
        reserve1,
        price1,
        price0,
        decimals0,
        decimals1,
        last_lp_reserve + sum(lp_transfer) over (partition by new.contract_address order by block_day asc) as lp_reserve
    from new
    left join old_last
        on new.contract_address = old_last.contract_address
    where
        latest = 1
        and project in ('pancakeswap', 'uniswap', 'sushiswap')
)

select
    *,
    (reserve0 * price0 / power(10, -decimals0) + reserve1 * price1 / power(10, -decimals1)) / lp_reserve as lp_price
from aggregate_transfer
