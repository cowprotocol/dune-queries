-- with cow_pools as (
select
concat('<a href="https://dune.com/cowprotocol/cow-amm-micro-v2?token_a=', cast(token_1_address as varchar), '&token_b=', cast(token_2_address as varchar), '&blockchain=', t.blockchain, '&ref_token_a=', cast(token_1_address as varchar), '&ref_token_b=', cast(token_2_address as varchar), '&ref_blockchain=', t.blockchain,
    '" target="_blank">', cast(address as varchar), '</a>') as cow_amm_address,
t.blockchain,
total_tvl,
t1.symbol as token_1_symbol,
t2.symbol as token_2_symbol,
365 * surplus_1d/total_tvl as "1d APY",
365 * surplus_7d / total_tvl / 7 as "7d APY",
365 * surplus_30d / total_tvl / 30 as "30d APY",
token_1_address,
token_2_address
from (
select
        -- created_at,
        address,
        blockchain,
        token_1_address,
        token_2_address,
        RANK() OVER (PARTITION BY token_1_address,token_2_address ORDER BY created_at DESC) as ranking

    from query_3959044
    ) t
    
inner join tokens.erc20 as t1 
on token_1_address=t1.contract_address
left join tokens.erc20 as t2 
on token_2_address=t2.contract_address
left join query_4062965 tvl 
on tvl.pool=address

 where t.ranking=1
 and total_tvl>0
 group by 1,2,3,4,5,6,7,8,9,10
 order by total_tvl desc
--  and token_1_address!=token_2_address