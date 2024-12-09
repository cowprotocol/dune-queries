-- Finds the largest Uni Style pools (Pancake, Sushi, Uni) and their TVLs
-- Parameters:
--  {{blockchain}}: The blockchain to query
--  {{number_of_pools}}: The number of largest pools to return

with pools as (
    select
        substr(data, 13, 20) as contract_address,
        substr(topic1, 13, 20) as token0,
        substr(topic2, 13, 20) as token1,
        case
            when
                contract_address in (
                    0x1097053Fd2ea711dad45caCcc45EfF7548fCB362,
                    0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e
                ) then 'pancakeswap'
            when
                contract_address in (
                    0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f,
                    0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9,
                    0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6
                )
                then 'uniswap'
            when
                contract_address in (
                    0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac,
                    0xc35DADB65012eC5796536bD9864eD8773aBc74C4,
                    0x71524B4f93c58fcbF659783284E38825f0622859
                )
                then 'sushiswap'
        end as project
    from {{blockchain}}.logs
    where
        topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
        and contract_address in
        (
            0x1097053Fd2ea711dad45caCcc45EfF7548fCB362, --eth, pancake
            0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e, --arb/bas, pancake
            0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f, --eth, uni
            0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9, --arb, uni
            0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6, --bas, uni
            0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac, --eth, sushi
            0xc35DADB65012eC5796536bD9864eD8773aBc74C4, --arb/gno, sushi
            0x71524B4f93c58fcbF659783284E38825f0622859 --bas, sushi
        )
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
        rank() over (partition by (logs.contract_address) order by block_time desc, index desc) as latest
    from {{blockchain}}.logs
    inner join pools
        on logs.contract_address = pools.contract_address
    where
        topic0 = 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1 -- Sync
)

select distinct
    s.contract_address as pool_address,
    project,
    token0,
    token1,
    reserve0,
    reserve1,
    evt_block_time,
    reserve0 * p0.price * power(10, -p0.decimals) + reserve1 * p1.price * power(10, -p1.decimals) as tvl
from syncs as s
inner join prices.day as p0
    on token0 = p0.contract_address
inner join prices.day as p1
    on token1 = p1.contract_address
where
    latest = 1
    and p0.timestamp = date_trunc('day', case when ('{{end_time}}' = '2100-01-01' or date('{{end_time}}') = date_trunc('day', now())) then now() - interval '1' day else date('{{end_time}}') end)
    and p1.timestamp = date_trunc('day', case when ('{{end_time}}' = '2100-01-01' or date('{{end_time}}') = date_trunc('day', now())) then now() - interval '1' day else date('{{end_time}}') end)
order by tvl desc
limit {{number_of_pools}}
