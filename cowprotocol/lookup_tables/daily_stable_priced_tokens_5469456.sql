-- this query returns a daily list of token addresses which fit into a few price stability criteria over the previous 30d
with
daily_prices as (
select
    date_trunc('day', timestamp) as date,
    contract_address as token_address,
    blockchain,
    min(price) as daily_min_price -- to avoid temporary pricing errors
from prices.hour
where
    timestamp >= timestamp '2022-01-01'
group by 1, 2, 3
)
, volatility as (
select
    date
    , token_address
    , blockchain
    , lag(date, 30) over (partition by token_address, blockchain order by date) as thirty_intervals
    , avg(daily_min_price) over (partition by token_address, blockchain order by date rows between 30 preceding and current row) as avg_price
    , stddev_pop(daily_min_price) over (partition by token_address, blockchain order by date rows between 30 preceding and current row) as stdev_price
    , max(daily_min_price) over (partition by token_address, blockchain order by date rows between 30 preceding and current row)
        - min(daily_min_price) over (partition by token_address, blockchain order by date rows between 30 preceding and current row) as price_range
from daily_prices
)
, filtered as (
select
    v.*
    , t.symbol
    , t.name
from volatility v
left join tokens.erc20 t
    on v.token_address = t.contract_address
    and v.blockchain = t.blockchain
where
    date_diff('day', thirty_intervals, date) < 35  --allow price to be missing in up to 5d
    and stdev_price / avg_price < 0.01   -- relative volatility < 1%
    and price_range / avg_price < 0.05    -- price range within 5%
)
select *
from filtered
