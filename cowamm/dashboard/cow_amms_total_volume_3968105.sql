--Query computes the trading volume over all Cow AMMs (matching users and rebalancing) and 7 day growth thereof.
with all_trades as (
    select * from cow_protocol_ethereum.trades
    union all
    select * from cow_protocol_gnosis.trades
    union all
    select * from cow_protocol_arbitrum.trades
    union all
    select * from cow_protocol_base.trades
),

cow_trades as (
    select
        block_date,
        sum(usd_value) as volume
    from all_trades
    inner join query_3959044 as pool
        on all_trades.trader = pool.address
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
