-- This table gets the reference pools to compare with for the CoW AMM from Uniswap, Suhsiswap and Pancakeswap
-- It also computes the reserves of the different assets before the creation fo the CowW AMM to simplify the later on computation

with cow_amm_pools as (
    select * from dune.cowprotocol.result_balancer_co_w_am_ms
),

uni_style_pools as(
    select
        u.created_at,
        u.contract_address,
        u.token0,
        u.token1,
        u.project,
        --cow amms can have multiple pools for a same pair
        min(c.created_at) as cow_created_at
    from "query_4420646(blockchain = 'ethereum')" as u
    inner join cow_amm_pools as c
        on 
            (u.token0 = c.token_1_address and u.token1 = c.token_2_address)
            or (u.token1 = c.token_1_address and u.token0 = c.token_2_address)
    group by 1, 2, 3, 4, 5
),

reserves as (
    select
        u.created_at,
        u.contract_address,
        u.token0,
        u.token1,
        u.project,
        u.cow_created_at,
        last_value(varbinary_to_uint256(substr(data, 1, 32))) over (partition by logs.contract_address order by logs.block_time asc) as reserve0,
        last_value(varbinary_to_uint256(substr(data, 33, 32))) over (partition by logs.contract_address order by logs.block_time asc) as reserve1,
        rank() over (partition by logs.contract_address order by logs.block_time desc, logs.index desc) as latest
    from uni_style_pools as u
    inner join ethereum.logs
        on logs.contract_address = u.contract_address
    where
        topic0 = 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1 -- Sync
        and logs.block_time >= u.created_at
        and date(logs.block_time) < date(u.cow_created_at)
        -- Assuming pool active in 2024 for execution optimization
        and logs.block_time >= timestamp '2024-01-01'
)


select
    u.created_at,
    u.cow_created_at,
    u.contract_address,
    u.token0,
    u.token1,
    u.project,
    reserve0,
    reserve1,
    sum(case when "from" = 0x0000000000000000000000000000000000000000 then value else -value end) as lp_transfer_reserve
from reserves as u
inner join erc20_ethereum.evt_transfer
    on evt_transfer.contract_address = u.contract_address
where
    latest = 1
    and ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
    and evt_block_time >= u.created_at
    and date(evt_block_time) <= date(u.cow_created_at)
    --Uniswap v2 launch
    and evt_block_time >= timestamp '2020-09-17'
group by 1,2,3,4,5,6,7,8