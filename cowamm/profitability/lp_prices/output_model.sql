--Summary of the output for cow amms and uniswap style

select
    block_day,
    contract_address,
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
    transfer0,
    transfer1,
    -- not useful in our internal database
    lp_reserve,
    lp_transfer,
    lp_price
from previous
