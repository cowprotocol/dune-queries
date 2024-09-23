-- Computes the swap fee per $100 tvl for each day (aka its invariant growth) for a Uni v2 pool.
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run

-- Limit the date range
with date_range as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '{{start}}'),
            date(now())
        )) t (day) --noqa: AL01
),

-- Finds the uniswap v2 pool address given tokens specified in query parameters (regardless of order)
pools as (
    select
        substr(data, 13, 20) as contract_address,
        substr(topic1, 13, 20) as token0,
        substr(topic2, 13, 20) as token1
    from {{blockchain}}.logs
    where
        topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9 -- PairCreated
        -- topic1: 0x0...0<token0>, topic2: 0x0...0<token1>
        and ((substr(topic1, 13, 20) = {{token_a}} and substr(topic2, 13, 20) = {{token_b}}) or (substr(topic2, 13, 20) = {{token_a}} and substr(topic1, 13, 20) = {{token_b}}))
),

syncs as (
    select
        tx_hash as evt_tx_hash,
        index as evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        contract_address,
        varbinary_to_uint256(substr(data, 1, 32)) as reserve0,
        varbinary_to_uint256(substr(data, 33, 32)) as reserve1,
        rank() over (partition by (contract_address) order by block_time desc) as latest
    from {{blockchain}}.logs
    where
        topic0 = 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1 -- Sync
        and contract_address in (select contract_address from pools)
),

-- select the pool with the largest latest k
pool as (
    select pools.*
    from pools
    inner join syncs
        on
            pools.contract_address = syncs.contract_address
            and latest = 1
    order by (reserve0 * reserve1) desc
    limit 1
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
        and contract_address = (select contract_address from pool)
),

-- gets the swapped volume and tvl at the time of the swap for each swap
tvl_volume_per_swap as (
    select
        syncs.evt_block_time,
        syncs.evt_tx_hash,
        (amount0In * p0.price / pow(10, p0.decimals)) + (amount1In * p1.price / pow(10, p1.decimals)) as volume_in,
        (amount0Out * p0.price / pow(10, p0.decimals)) + (amount1Out * p1.price / pow(10, p1.decimals)) as volume_out,
        (reserve0 * p0.price / pow(10, p0.decimals)) + (reserve1 * p1.price / pow(10, p1.decimals)) as tvl
    from syncs
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
            and p0.contract_address = token0
    inner join prices.usd as p1
        on
            date_trunc('minute', syncs.evt_block_time) = p1.minute
            and p1.contract_address = token1
    where syncs.evt_block_time >= date(timestamp '{{start}}')
)

select
    day,
    sum((volume_in + volume_out) / 2) as volume,
    sum((volume_in + volume_out) / 2) * 0.003 as absolute_invariant_growth,
    avg(tvl) as tvl,
    sum((volume_in + volume_out) / 2 / tvl) * 0.003 as pct_invariant_growth
from date_range as dr
left join tvl_volume_per_swap
    on dr.day = date(evt_block_time)
group by day
order by day desc
