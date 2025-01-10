--Query computes the trading volume over all Cow AMMs (matching users and rebalancing)
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
        date_trunc('{{frequence}}', block_date) as period,
        sum(usd_value) as volume
    from all_trades
    inner join query_3959044 as pool
        on all_trades.trader = pool.address
    group by 1
)

select * from cow_trades
