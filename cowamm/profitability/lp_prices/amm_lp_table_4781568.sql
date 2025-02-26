with cow_amms as (
    select
        'ethereum' as blockchain, *
    from "query_4420687(blockchain = 'ethereum', start = '2024-07-29', end = '2100-01-01')"
    union all
    select
        'gnosis' as blockchain, *
    from "query_4420687(blockchain = 'gnosis', start = '2024-07-29', end = '2100-01-01')"
    union all
    select
        'arbitrum' as blockchain, *
    from "query_4420687(blockchain = 'arbitrum', start = '2024-07-29', end = '2100-01-01')"
    union all
    select
        'base' as blockchain, *
    from "query_4420687(blockchain = 'base', start = '2024-07-29', end = '2100-01-01')"
),

uni_style_pools as (
    select
        'ethereum' as blockchain, *
    from "query_4420675(blockchain = 'ethereum', start = '2024-07-29', end = '2100-01-01')"
    union all
    select
        'gnosis' as blockchain, *
    from "query_4420675(blockchain = 'gnosis', start = '2024-07-29', end = '2100-01-01')"
    union all
    select
        'arbitrum' as blockchain, *
    from "query_4420675(blockchain = 'arbitrum', start = '2024-07-29', end = '2100-01-01')"
    union all
    select
        'base' as blockchain, *
    from "query_4420675(blockchain = 'base', start = '2024-07-29', end = '2100-01-01')"
)

select
    created_at,
    created_at as cow_created_at,
    blockchain,
    contract_address,
    'cow_amm' as project,
    token0,
    token1,
    weight0,
    weight1,
    day,
    lp_reserve,
    reserve0,
    reserve1,
    price0,
    price1,
    decimals0,
    decimals1
from cow_amms

union all

select
    uni.created_at,
    uni.cow_created_at,
    uni.blockchain,
    uni.contract_address,
    uni.project,
    uni.token0,
    uni.token1,
    50 as weight0,
    50 as weight1,
    uni.day,
    uni.lp_reserve,
    uni.reserve0,
    uni.reserve1,
    cow.price0,
    cow.price1,
    cow.decimals0,
    cow.decimals1
from uni_style_pools as uni
inner join cow_amms as cow
    on uni.blockchain = cow.blockchain
    and (
        (uni.token0 = cow.token0 and uni.token1 = cow.token1)
        or (uni.token1 = cow.token0 and uni.token0 = cow.token1)
    )
    and uni.day = cow.day
    and uni.cow_created_at = cow.created_at
