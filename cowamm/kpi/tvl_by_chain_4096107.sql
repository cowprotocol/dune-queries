-- This query computes the total TVL evolution per day for all CoW AMM pools for a given chain
-- Parameters
--  {{blockchain}} - the target chain

with pools as (
    select
        created_at,
        token_1_address,
        token_2_address,
        address
    from query_3959044
    where blockchain = '{{blockchain}}'
),

date_range as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '2024-07-30'),
            date(now())
        )) t (day) --noqa: AL01
),

-- Computes the token balance changes of the relevant token per day
reserves_delta as (
    select
        p.address as pool,
        contract_address as token,
        date(evt_block_time) as "day",
        sum(case when "from" = p.address then -value else value end) as amount
    from erc20_{{blockchain}}.evt_transfer as t
    inner join pools as p
        on
            (
                t."from" = p.address
                or t.to = p.address
            )
            and (token_1_address = contract_address or token_2_address = contract_address)
    group by 1, 2, 3
),

-- Sums up token deltas by day, potentially leaving days without an entry
reserves_by_day_incomplete as (
    select
        day,
        pool,
        token,
        sum(amount) over (partition by (pool, token) order by day) as balance
    from reserves_delta
),

-- Reserves by day without gaps (if you filter on latest = 1)
reserves_by_day as (
    select
        dr.day,
        pool,
        token,
        balance,
        rank() over (partition by (dr.day) order by r.day desc) as latest
    from date_range as dr
    inner join reserves_by_day_incomplete as r
        on dr.day >= r.day
),

-- take the balances at the end of each day and multiply it with closing price to get tvl
tvl as (
    select
        r.day,
        pool,
        token,
        balance,
        (balance * p.price_close) / pow(10, decimals) as tvl
    from reserves_by_day as r
    inner join prices.day as p
        on
            r.day = p.day
            and p.contract_address = token
    where latest = 1
)

select
    day,
    sum(tvl) as tvl
from tvl
group by 1
order by 1 desc
