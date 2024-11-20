-- Computes volume, tvl and APR for Sushiswap pools
-- APR is measured as the fees earned per $ invested, over the last 24 hours, projected over 1 year
-- Input: blockchain

with
-- select the pool with the largest latest k
pool as (
    select
        contract_address,
        token0,
        token1
    from "query_4227247(blockchain='{{blockchain}}')"
    where latest = 1
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
        syncs.contract_address,
        syncs.evt_block_time,
        syncs.evt_tx_hash,
        (amount0In * p0.price / pow(10, p0.decimals)) + (amount1In * p1.price / pow(10, p1.decimals)) as volume_in,
        (amount0Out * p0.price / pow(10, p0.decimals)) + (amount1Out * p1.price / pow(10, p1.decimals)) as volume_out,
        (reserve0 * p0.price / pow(10, p0.decimals)) + (reserve1 * p1.price / pow(10, p1.decimals)) as tvl
    from "query_4227247(blockchain='{{blockchain}}')" as syncs
    inner join swaps
        on
            syncs.evt_tx_hash = swaps.evt_tx_hash
            and syncs.contract_address = swaps.contract_address
            and syncs.evt_index + 1 = swaps.evt_index
    inner join pool
        on syncs.contract_address = pool.contract_address
    inner join prices.usd as p0
        on
            date_trunc('minute', syncs.evt_block_time) = p0.minute
            and syncs.token0 = p0.contract_address
    inner join prices.usd as p1
        on
            date_trunc('minute', syncs.evt_block_time) = p1.minute
            and syncs.token1 = p1.contract_address
    where syncs.evt_block_time >= date(date_add('day', -7, now()))
)

select
    contract_address,
    sum((volume_in + volume_out) / 2) as volume,
    avg(tvl) as tvl,
    365 * sum((volume_in + volume_out) / 2 / tvl) * 0.003 as apr,
    0.003 as fee
from tvl_volume_per_swap
where evt_block_time >= date_add('day', -1, now())
group by contract_address

union distinct
select
    pool_address as contract_address,
    0 as volume,
    tvl,
    0 as apr,
    0.003 as fee
from "query_4223554(blockchain='{{blockchain}}')"
where pool_address not in (select contract_address from tvl_volume_per_swap)
