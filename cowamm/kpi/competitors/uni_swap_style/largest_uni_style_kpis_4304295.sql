-- Computes volume, tvl and APR for Uni Swap style pools (Uni, Pancake, Sushi)
-- APR is measured as the fees earned per $ invested, over the last 24 hours, projected over 1 year
-- The type of pool (Uni/Pancake/Sushi) is not specified
-- Parameters:
-- {{blockchain}}: The blockchain to query

-- select the pool with the largest latest k
with pool as (
    select
        pool_address as contract_address,
        token0,
        token1,
        tvl
    from "query_4303563(blockchain='{{blockchain}}', number_of_pools = '{{number_of_pools}}')"
),

swaps as (
    select
        tx_hash as evt_tx_hash,
        index as evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        contract_address,
        varbinary_to_uint256(substr(data, 1, 32)) as amount0In,
        varbinary_to_uint256(substr(data, 33, 32)) as amount1In,
        varbinary_to_uint256(substr(data, 65, 32)) as amount0Out,
        varbinary_to_uint256(substr(data, 97, 32)) as amount1Out
    from {{blockchain}}.logs
    where
        topic0 = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822 -- Swap
        and contract_address in (select contract_address from pool)
),

-- gets the swapped volume and tvl at the time of the swap for each swap
tvl_volume_per_swap as (
    select
        syncs.pool_address,
        syncs.evt_block_time,
        syncs.evt_tx_hash,
        (amount0In * p0.price / pow(10, p0.decimals)) + (amount1In * p1.price / pow(10, p1.decimals)) as volume_in,
        (amount0Out * p0.price / pow(10, p0.decimals)) + (amount1Out * p1.price / pow(10, p1.decimals)) as volume_out,
        (reserve0 * p0.price / pow(10, p0.decimals)) + (reserve1 * p1.price / pow(10, p1.decimals)) as tvl
    from "query_4304212(blockchain='{{blockchain}}', number_of_pools = '{{number_of_pools}}')" as syncs
    inner join swaps
        on
            syncs.evt_tx_hash = swaps.evt_tx_hash
            and syncs.pool_address = swaps.contract_address
            and syncs.evt_index + 1 = swaps.evt_index
    inner join pool
        on syncs.pool_address = pool.contract_address
    inner join prices.usd as p0
        on
            date_trunc('minute', syncs.evt_block_time) = p0.minute
            and syncs.token0 = p0.contract_address
    inner join prices.usd as p1
        on
            date_trunc('minute', syncs.evt_block_time) = p1.minute
            and syncs.token1 = p1.contract_address
    where
        syncs.evt_block_time >= date(date_add('day', -1, now()))
        and swaps.evt_block_time >= date(date_add('day', -1, now()))
)

select
    contract_address,
    0.003 as fee,
    coalesce(sum((volume_in + volume_out) / 2), 0) as volume,
    -- the average pool is conceptually unnecessary because the table pool has only one tvl per pool
    -- but it is necessary for the group by statement
    avg(pool.tvl) as tvl,
    coalesce(365 * sum((volume_in + volume_out) / 2 / t.tvl) * 0.003, 0) as apr
from pool
left join tvl_volume_per_swap as t
    on contract_address = pool_address
group by contract_address