-- merge the old lines in the data base with the additional ones
-- compute the reserves/ lp reserves and prices

with old_last as (
    select
        contract_address,
        reserve0 as last_reserve0,
        reserve1 as last_reserve1,
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
        price1,
        price0,
        decimals0,
        decimals1,
        coalesce(last_reserve0, 0) + sum(transfer0) over (partition by new.contract_address order by block_day asc) as reserve0,
        coalesce(last_reserve1, 0) + sum(transfer1) over (partition by new.contract_address order by block_day asc) as reserve1,
        coalesce(last_lp_reserve, 0) + sum(lp_transfer) over (partition by new.contract_address order by block_day asc) as lp_reserve
    from new
    left join old_last
        on new.contract_address = old_last.contract_address
    where
        latest = 1
        and project = 'CoW AMM'
)

select
    *,
    -- if 1 token does not have a price, we assume the pool is balanced
    coalesce(
        (reserve0 * price0 / power(10, -decimals0) + reserve1 * price1 / power(10, -decimals1)),
        reserve0 * price0 / power(10, -decimals0),
        reserve1 * price1 / power(10, -decimals1)
    ) / lp_reserve as lp_price
from aggregate_transfer

-- Then, drop lp_transfer, transfer0 and transfer1
-- And merge with old database
