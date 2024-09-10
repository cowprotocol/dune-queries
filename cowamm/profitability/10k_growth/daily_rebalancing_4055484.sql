-- Computes the balances and current value of a counterfactual portfolio that invests 10k evenly into two tokens and re-balances once a day to keep a 50:50 exposure
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run

-- Note: not using a simpler recursive approach due to Dune's recursion depth limitation.
-- Current value of initial investment can be computed as the product of cumulative price changes per day, since
-- `value(day+1) = value(day) * (p1.price(day+1)/p1.price(day) + p2.price(day+1) / p2.price(day))/2`
-- Thus, current_value can be computed using cumulative products of (sums of) daily price changes:
-- `(p1.price(day+1)/p1.price(day) + p2.price(day+1)/p2.price(day))/2`

-- limit the relevant date range
with date_series as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '{{start}}'),
            date(now())
        )) t (day) --noqa: AL01
),

-- computes relative daily price changes for both assets
daily_price_change as (
    select
        ds.day,
        p1.price_close / previous_p1.price_close as p1,
        p2.price_close / previous_p2.price_close as p2
    from date_series as ds
    inner join prices.usd_daily as p1
        on
            p1.day = ds.day
            and p1.contract_address = {{token_a}}
            and p1.blockchain = 'ethereum'
    inner join prices.usd_daily as previous_p1
        on
            previous_p1.day = ds.day - interval '1' day
            and previous_p1.contract_address = {{token_a}}
            and previous_p1.blockchain = 'ethereum'
    inner join prices.usd_daily as p2
        on
            p2.day = ds.day
            and p2.contract_address = {{token_b}}
            and p2.blockchain = 'ethereum'
    inner join prices.usd_daily as previous_p2
        on
            previous_p2.day = ds.day - interval '1' day
            and previous_p2.contract_address = {{token_b}}
            and previous_p2.blockchain = 'ethereum'
)

-- For each day multiply initial investment with cumulative product of average price change of the two assets
select
    day,
    -- SQL doesn't support PRODUCT() over (...), but luckily "the sum of logarithms" is equal to "logarithm of the product",
    exp(sum(ln((p1 + p2) / 2)) over (order by day asc)) * 10000 as current_value_of_investment
from daily_price_change
order by 1 desc
