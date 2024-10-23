-- Computes the balances and their dollar value of a all CoW AMM pools for a specific token pair at every transaction where it changed
-- Parameters
--  {{token_a}} - either token of the pool
--  {{token_b}} - other token of the pool

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
    ) where ranking=1
    -- and token_1_address!=token_2_address
),

-- Computes the token balance changes of the relevant token per transaction
reserves_delta as (
    select
        evt_block_time,
        evt_tx_hash,
        p.address as pool,
        contract_address as token,
        MAX(evt_block_number) as evt_block_number,
        MAX(evt_index) as evt_index,
        SUM(case when "from" = p.address then -value else value end) as amount
    from erc20_ethereum.evt_transfer as t
    inner join cow_amm_pool as p
        on
           ( t."from" = p.address
            or t.to = p.address)
            
            and contract_address in (token_1_address, token_2_address)
    group by 1, 2, 3, 4
    
    union all 
    select
        evt_block_time,
        evt_tx_hash,
        p.address as pool,
        contract_address as token,
        MAX(evt_block_number) as evt_block_number,
        MAX(evt_index) as evt_index,
        SUM(case when "from" = p.address then -value else value end) as amount
    from erc20_gnosis.evt_transfer as t
    inner join cow_amm_pool as p
        on
           ( t."from" = p.address
            or t.to = p.address)
            
            and contract_address in (token_1_address, token_2_address)
    group by 1, 2, 3, 4
    
    union all 
    select
        evt_block_time,
        evt_tx_hash,
        p.address as pool,
        contract_address as token,
        MAX(evt_block_number) as evt_block_number,
        MAX(evt_index) as evt_index,
        SUM(case when "from" = p.address then -value else value end) as amount
    from erc20_arbitrum.evt_transfer as t
    inner join cow_amm_pool as p
        on
           ( t."from" = p.address
            or t.to = p.address)
            
            and contract_address in (token_1_address, token_2_address)
    group by 1, 2, 3, 4
),

-- sums token balance changes to get total balances of relevant tokens per transaction
balances_by_tx as (
    select
        evt_block_time,
        evt_tx_hash,
        pool,
        token,
        SUM(amount) over (partition by (pool, token) order by (evt_block_number, evt_index)) as balance
    from reserves_delta
),

-- joins token balances with prices to get tvl of pool per transaction
tvl as (
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        b.pool,
        b.token,
        balance,
        price,
        balance * price / POW(10, decimals) as tvl
    from balances_by_tx as b
    inner join prices.usd as p
        on
            p.minute = DATE_TRUNC('minute', evt_block_time)
            and b.token = p.contract_address
)

, trades as (
    select * from cow_protocol_ethereum.trades where trader in (select address from cow_amm_pool)
    union all
    select * from cow_protocol_gnosis.trades where trader in (select address from cow_amm_pool)
    union all
    select * from cow_protocol_arbitrum.trades where trader in (select address from cow_amm_pool)
)

, surplus as (
    select
        trader as pool,
        date(block_time) as day,
        sum(surplus_usd) as total_surplus
    from trades
    group by 1,2
)

, final as (
select pool, date(block_time) as day, sum(tvl) as total_tvl
    from (
        select 
            pool, 
            date(tvl.block_time) as day, 
            tvl.block_time, 
            tvl,
            RANK() OVER (PARTITION BY pool, date(tvl.block_time) ORDER BY tvl.block_time DESC) as ranking
        from tvl
        ) tvl
    where ranking=1
group by 1,2
)

select 
    pool, 
    total_tvl,
    surplus_1d,
    surplus_7d,
    surplus_30d
from (
    select 
        tvl.pool, 
        total_tvl,
        sum(total_surplus) OVER (PARTITION BY surplus.pool ORDER BY surplus.day DESC GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) as surplus_1d,
        sum(total_surplus) OVER (PARTITION BY surplus.pool ORDER BY surplus.day DESC GROUPS BETWEEN 1 FOLLOWING AND 7 FOLLOWING) as surplus_7d,
        sum(total_surplus) OVER (PARTITION BY surplus.pool ORDER BY surplus.day DESC GROUPS BETWEEN 1 FOLLOWING AND 30 FOLLOWING) as surplus_30d,
        RANK() OVER (PARTITION BY tvl.pool ORDER BY tvl.day DESC) as ranking
    from final tvl
    join surplus
      on surplus.day = tvl.day
      and tvl.pool = surplus.pool
)
where ranking=1