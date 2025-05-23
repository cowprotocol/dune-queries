-- Query computes the trading volume over all CoW AMMs (matching users and rebalancing)

-- Parameters:
-- {{aggregate_by}}: the frequency of the data, e.g. 'day', 'week', 'month'
with cow_trades as (
    select
        date_trunc('{{aggregate_by}}', block_date) as period,
        sum(amount_usd) as volume
    from cow_protocol.trades as t
    inner join dune.cowprotocol.result_balancer_co_w_am_ms as pool
        on t.taker = pool.address
    group by 1
)

select * from cow_trades
