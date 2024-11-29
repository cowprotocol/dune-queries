-- Computes the balances and current value of a counterfactual portfolio that invests 10k evenly into two tokens and holds them
-- Parameters
--  {{token_a}} - either token
--  {{token_b}} - other token
--  {{start}} - date as of which the analysis should run

-- limit the relevant date range
with date_series as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '{{start}}'),
            date(now())
        )) t (day) --noqa: AL01
),

starting_balance as (
    select
        5000 / p1.price_close as token_a_start,
        5000 / p2.price_close as token_b_start
    from prices.day as p1
    inner join prices.day as p2
        on
            p1.timestamp = p2.timestamp
            and p1.timestamp = date(timestamp '{{start}}')
            and p1.contract_address = {{token_a}}
            and p2.contract_address = {{token_b}}
)

select
    ds.day,
    token_a_start * p1.price_close + token_b_start * p2.price_close as current_value_of_investment
from starting_balance
cross join date_series as ds
inner join prices.day as p1
    on
        ds.day = p1.timestamp
        and p1.contract_address = {{token_a}}
inner join prices.day as p2
    on
        ds.day = p2.timestamp
        and p2.contract_address = {{token_b}}
order by 1 desc
