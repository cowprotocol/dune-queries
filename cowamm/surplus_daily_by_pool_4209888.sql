with cow_amm_pool as (
    select *
from (
select
        created_at,
        address,
        token_1_address,
        token_2_address,
        RANK() OVER (PARTITION BY token_1_address,token_2_address ORDER BY created_at DESC) as ranking

    from query_3959044
    ) where ranking=1 --we only want the most recent pool
),

trades as (
    select 'ethereum' as blockchain, * from cow_protocol_ethereum.trades where trader in (select address from cow_amm_pool)
    union all
    select 'gnosis' as blockchain, * from cow_protocol_gnosis.trades where trader in (select address from cow_amm_pool)
    union all
    select 'arbitrum' as blockchain, * from cow_protocol_arbitrum.trades where trader in (select address from cow_amm_pool)
)


select
    blockchain,
    trader as pool,
    date(block_time) as day,
    sum(surplus_usd) as total_surplus
from trades
group by 1,2,3