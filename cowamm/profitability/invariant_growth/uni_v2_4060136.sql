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

-- select the pool with the largest latest k
pool as (
    select
        contract_address,
        token0,
        token1
    from "query_4117043(blockchain='{{blockchain}}',token_a='{{token_a}}',token_b='{{token_b}}')"
    where latest = 1
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
    from "query_4117043(blockchain='{{blockchain}}',token_a='{{token_a}}',token_b='{{token_b}}')" as syncs
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
