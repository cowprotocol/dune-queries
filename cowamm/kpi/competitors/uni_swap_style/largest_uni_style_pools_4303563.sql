-- Finds the largest Uni Style pools (Pancake, Sushi, Uni) and their TVLs
-- Parameters:
--  {{blockchain}}: The blockchain to query
--  {{number_of_pools}}: The number of largest pools to return

with pools as (
    select
        substr(data, 13, 20) as contract_address,
        substr(topic1, 13, 20) as token0,
        substr(topic2, 13, 20) as token1
    from {{blockchain}}.logs
    where
        topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
),

syncs as (
    select
        pools.*,
        tx_hash as evt_tx_hash,
        index as evt_index,
        block_number as evt_block_number,
        date_trunc('minute', block_time) as evt_block_time,
        varbinary_to_uint256(substr(data, 1, 32)) as reserve0,
        varbinary_to_uint256(substr(data, 33, 32)) as reserve1,
        rank() over (partition by (logs.contract_address) order by block_time desc, index asc) as latest
    from {{blockchain}}.logs
    inner join pools
        on logs.contract_address = pools.contract_address
    where
        topic0 = 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1 -- Sync
)

select distinct
    s.contract_address as pool_address,
    token0,
    token1,
    reserve0,
    reserve1,
    evt_block_time,
    reserve0 * p0.price * power(10, -p0.decimals) + reserve1 * p1.price * power(10, -p1.decimals) as tvl
from syncs as s
inner join prices.usd as p0
    on
        token0 = p0.contract_address
        and p0.minute = evt_block_time
inner join prices.usd as p1
    on
        token1 = p1.contract_address
        and p1.minute = evt_block_time
where latest = 1
order by 7 desc
limit {{number_of_pools}}
