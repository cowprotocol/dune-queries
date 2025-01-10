--Query computes the trading volume over all Cow AMMs (matching users and rebalancing)

-- Parameters:
-- {{aggregate_by}}: the frequence of the data, e.g. 'day', 'week', 'month'
with cow_trades as (
    select
        date_trunc('{{aggregate_by}}', block_date) as period,
        sum(amount_usd) as volume
    from cow_protocol.trades as t
    inner join query_3959044 as pool
        on t.taker = pool.address
    group by 1
)

select * from cow_trades
