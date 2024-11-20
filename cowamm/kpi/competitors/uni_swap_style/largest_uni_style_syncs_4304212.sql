-- This query finds all the sync event which happens after a swap and reference the new reserves of the pool
-- Parameters:
-- {{blockchain}}: The blockchain to query

-- Finds the pools with the largest tvl
with pools as (
    select
        pool_address,
        token0,
        token1
    from "query_4303563(blockchain='{{blockchain}}', number_of_pools = '{{number_of_pools}}')"
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
    on logs.contract_address = pools.pool_address
where
    topic0 = 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1 -- Sync
    and block_time >= date(date_add('day', -1, now()))
