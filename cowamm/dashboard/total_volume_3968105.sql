-- Query computes the trading volume over all CoW AMMs (matching users and rebalancing) and 7 day growth thereof.
with cow_trades as (
    select
        block_date,
        sum(amount_usd) as volume
    from cow_protocol.trades as t
    inner join dune.cowprotocol.result_balancer_co_w_am_ms as pool
        on t.taker = pool.address
    group by 1
),

cumulative_volume as (
    select
        block_date as day, --noqa: RF04
        sum(volume) over (order by block_date) as tvl
    from cow_trades
)

select
    prev.tvl as prev,
    curr.tvl as curr,
    100 * (curr.tvl - prev.tvl) / prev.tvl as growth
from cumulative_volume as curr
inner join cumulative_volume as prev
    on curr.day = prev.day + interval '7' day
    -- we don't have data for today
    and curr.day = date(now()) - interval '1' day
