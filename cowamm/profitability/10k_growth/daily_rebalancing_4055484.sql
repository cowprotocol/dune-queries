-- Computes the balances and current value of a counterfactual portfolio that invests 10k evenly into two tokens and re-balances once a day to keep a 50:50 exposure
-- Parameters
--  {{token_a}} - either token
--  {{token_b}} - other token
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
        p1.price / previous_p1.price as p1,
        p2.price / previous_p2.price as p2
    from date_series as ds
    inner join prices.day as p1
        on
            ds.day = p1.timestamp
            and p1.contract_address = {{token_a}}
    left join prices.day as previous_p1
        on
            previous_p1.timestamp = ds.day - interval '1' day
            -- avoid computing price change on first day
            and previous_p1.timestamp >= date(timestamp '{{start}}')
            and previous_p1.contract_address = {{token_a}}
    inner join prices.day as p2
        on
            ds.day = p2.timestamp
            and p2.contract_address = {{token_b}}
    left join prices.day as previous_p2
        on
            previous_p2.timestamp = ds.day - interval '1' day
            -- avoid computing price change on first day
            and previous_p2.timestamp >= date(timestamp '{{start}}')
            and previous_p2.contract_address = {{token_b}}
)

-- For each day multiply initial investment with cumulative product of average price change of the two assets
select
    day,
    -- SQL doesn't support PRODUCT() over (...), but luckily "the sum of logarithms" is equal to "logarithm of the product",
    -- coalesce to factor 1 on first day
    coalesce(exp(sum(ln((p1 + p2) / 2)) over (order by day asc)), 1) * 10000 as current_value_of_investment
from daily_price_change
order by 1 desc
