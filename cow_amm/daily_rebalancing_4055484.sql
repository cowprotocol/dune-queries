-- Computes the balances and current value of a counterfactual portfolio that invests 10k evenly into two tokens and re-balances once a day to keep a 50:50 exposure
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run

with recursive balances (day, balance0, balance1) as (
    -- Base case: $10k are invested evenly into token_a and token_b at the opening price of the start day
    select
        date(timestamp '{{start}}'),
        10000 / 2 / p1.price_open, -- Initial balance in token_a
        10000 / 2 / p2.price_open  -- Initial balance in token_b
    from prices.usd_daily as p1
    inner join prices.usd_daily as p2
        on
            p1.day = date(timestamp '{{start}}')
            and p1.day = p2.day
            and p1.blockchain = 'ethereum'
            and p2.blockchain = 'ethereum'
            and p1.contract_address = {{token_a}}
            and p2.contract_address = {{token_b}}

    union all

    -- Recursive case: Compute the next day's balances according to previous day's closing price and reinvest half into each token
    select
        b.day + interval '1' day,
        (balance0 * p1.price_close + balance1 * p2.price_close) / 2 / p1.price_close, -- Updated balance in token_a
        (balance0 * p1.price_close + balance1 * p2.price_close) / 2 / p2.price_close  -- Updated balance in token_b
    from balances as b
    inner join prices.usd_daily as p1
        on b.day = p1.day and p1.contract_address = {{token_a}}
    inner join prices.usd_daily as p2
        on b.day = p2.day and p2.contract_address = {{token_b}}
    where b.day < date(now())
)

-- Multiply daily balances with end of day closing price to get current value
select
    b.day,
    b.balance0,
    b.balance1,
    (b.balance0 * p1.price_close) + (b.balance1 * p2.price_close) as current_value
from balances as b
inner join prices.usd_daily as p1
    on b.day = p1.day and p1.contract_address = {{token_a}}
inner join prices.usd_daily as p2
    on b.day = p2.day and p2.contract_address = {{token_b}}
order by 1 desc;
