--Groups all the data in one table to then materialize it in a view
--Easy to add new AMMs from there

-- noqa: disable=all
with cow_amms as (
    select
        'ethereum' as blockchain, t.*
    from "query_4420687(blockchain = 'ethereum', start = '2024-07-29', end = '2100-01-01')" as t
    union all
    select
        'gnosis' as blockchain, t.*
    from "query_4420687(blockchain = 'gnosis', start = '2024-07-29', end = '2100-01-01')" as t
    union all
    select
        'arbitrum' as blockchain, t.*
    from "query_4420687(blockchain = 'arbitrum', start = '2024-07-29', end = '2100-01-01')" as t
    union all
    select
        'base' as blockchain, t.*
    from "query_4420687(blockchain = 'base', start = '2024-07-29', end = '2100-01-01')" as t
),

uni_style_pools as (
    select
        'ethereum' as blockchain, t.*
    from "query_4420675(blockchain = 'ethereum', start = '2024-07-29', end = '2100-01-01')" as t
    union all
    select
        'gnosis' as blockchain, t.*
    from "query_4420675(blockchain = 'gnosis', start = '2024-07-29', end = '2100-01-01')" as t
    union all
    select
        'arbitrum' as blockchain, t.*
    from "query_4420675(blockchain = 'arbitrum', start = '2024-07-29', end = '2100-01-01')" as t
    union all
    select
        'base' as blockchain, t.*
    from "query_4420675(blockchain = 'base', start = '2024-07-29', end = '2100-01-01')" as t
)

fselect
    created_at,
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
    price0,
    price1,
    decimals0,
    decimals1
from uni_style_pools as uni