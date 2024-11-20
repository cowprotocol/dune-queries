-- Finds the sushiswap v2 pool address given tokens specified in query parameters (regardless of order)
with pools as (
    select
        substr(data, 13, 20) as contract_address,
        substr(topic1, 13, 20) as token0,
        substr(topic2, 13, 20) as token1
    from {{blockchain}}.logs
    where
        topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9 -- PairCreated
        -- topic1: 0x0...0<token0>, topic2: 0x0...0<token1>
        and substr(data, 13, 20) in (select pool_address from "query_4223554(blockchain='{{blockchain}}')")
)

select
    pools.*,
    tx_hash as evt_tx_hash,
    index as evt_index,
    block_time as evt_block_time,
    block_number as evt_block_number,
    varbinary_to_uint256(substr(data, 1, 32)) as reserve0,
    varbinary_to_uint256(substr(data, 33, 32)) as reserve1,
    rank() over (partition by (logs.contract_address) order by block_time desc) as latest
from {{blockchain}}.logs
inner join pools
    on logs.contract_address = pools.contract_address
where
    topic0 = 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1 -- Sync
