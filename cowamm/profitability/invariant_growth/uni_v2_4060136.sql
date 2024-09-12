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
pool as (
    select
        pool as contract_address,
        token0,
        token1
    from uniswap_ethereum.pools
    where
        ((token0 = {{token_a}} and token1 = {{token_b}}) or (token1 = {{token_a}} and token0 = {{token_b}}))
        and version = 'v2'
    limit 1
),

-- gets the swapped volume and tvl at the time of the swap for each swap
swaps as (
    select
        sync.evt_block_time,
        sync.evt_tx_hash,
        (amount0In * p0.price / pow(10, p0.decimals)) + (amount1In * p1.price / pow(10, p1.decimals)) as volume_in,
        (amount0Out * p0.price / pow(10, p0.decimals)) + (amount1Out * p1.price / pow(10, p1.decimals)) as volume_out,
        (reserve0 * p0.price / pow(10, p0.decimals)) + (reserve1 * p1.price / pow(10, p1.decimals)) as tvl
    from uniswap_v2_ethereum.Pair_evt_Sync as sync
    inner join uniswap_v2_ethereum.Pair_evt_Swap as swap
        on
            sync.evt_tx_hash = swap.evt_tx_hash
            and sync.contract_address = swap.contract_address
    inner join pool
        on sync.contract_address = pool.contract_address
    inner join prices.usd as p0
        on
            date_trunc('minute', sync.evt_block_time) = p0.minute
            and p0.contract_address = token0
    inner join prices.usd as p1
        on
            date_trunc('minute', sync.evt_block_time) = p1.minute
            and p1.contract_address = token1
    where sync.evt_block_time < date(timestamp '{{start}}')
)

select
    day,
    sum((volume_in + volume_out) / 2) as volume,
    sum((volume_in + volume_out) / 2) * 0.003 as absolute_invariant_growth,
    avg(tvl) as tvl,
    sum((volume_in + volume_out) / 2 / tvl) * 0.003 * 100 as pct_invariant_growth
from date_range as dr
left join swaps
    on dr.day = date(evt_block_time)
group by 1
