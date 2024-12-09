-- Computes the TVL, lp token total supply and lp token price of a CoW AMM pool over time
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run
--  {{blockchain}} - chain for which the query is

-- Given that we might not have records every day in the source data (e.g. not every day the lp supply may change), 
-- but still want to visualize development on a per day basis,  we create an auxiliary table with one record per 
-- day between `start` and `now`
with date_range as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '{{start}}'),
            date(now())
        )) t (day) --noqa: AL01
),

-- Finds the CoW AMM pool address given tokens specified in query parameters (regardless of order) 
cow_amm_pool as (
    select
        created_at,
        address
    from query_3959044
    where ((token_1_address = {{token_a}} and token_2_address = {{token_b}}) or (token_2_address = {{token_a}} and token_1_address = {{token_b}}))
    order by 1 desc
    limit 1
),

-- per day lp token total supply changes of the CoW AMM pool by looking at burn/mint events
lp_balance_delta as (
    select
        date(evt_block_time) as "day",
        sum(case when "from" = 0x0000000000000000000000000000000000000000 then value else -value end) as lp_supply
    from erc20_{{blockchain}}.evt_transfer
    where
        contract_address in (select address from cow_amm_pool)
        and ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
    group by 1
),

lp_total_supply_incomplete as (
    select
        day,
        lp_supply,
        sum(lp_supply) over (order by day) as total_lp
    from lp_balance_delta
),

-- performance optimisation: reduce the total range of the join below to the last value before the start period
lp_total_supply_start as (
    select max(day) as "start"
    from lp_total_supply_incomplete
    where day <= date(timestamp '{{start}}')
),

-- lp token total supply without date gaps
lp_total_supply as (
    select
        day,
        total_lp
    from (
        -- join full date range with potentially incomplete data. This results in many rows per day (all total supplies on or before that day)
        -- rank() is then used to order join candidates by recency (rank = 1 is the latest lp supply)
        select
            date_range.day,
            total_lp,
            rank() over (partition by (date_range.day) order by lp.day desc) as latest
        from date_range
        inner join lp_total_supply_incomplete as lp
            on date_range.day >= lp.day
            -- performance optimisation: this assumes one week prior to start there was at least one lp supply change event
            and lp.day >= (select start from lp_total_supply_start)
    )
    where latest = 1
),

tvl_by_tx as (
    select
        *,
        rank() over (partition by date(block_time) order by block_time desc) as latest
    from "query_4059700(token_a='{{token_a}}', token_b='{{token_b}}', blockchain='{{blockchain}}')"
    where pool = (select address from cow_amm_pool)
    -- performance optimisation: this assumes one week prior to start there was at least one tvl change event
    and block_time >= timestamp '{{start}}' - interval '7' day
),

-- take the balances of the pool at the end of each day and multiply it with closing price to get tvl
tvl as (
    select
        tvl_complete.day,
        balance1 * p1.price + balance2 * p2.price as tvl
    from (
        -- join full date range with potentially incomplete data. This results in many rows per day (all pool balances on or before that day)
        -- rank() is then used to order join candidates by recency (rank = 1 is the latest pool balances)
        select
            date_range.day,
            token1,
            balance1,
            token2,
            balance2,
            rank() over (partition by (date_range.day) order by tvl.block_time desc) as latest
        from date_range
        inner join tvl_by_tx as tvl
            on date_range.day >= date(tvl.block_time)
            -- performance optimisation: only look at the last update of the day
            and tvl.latest = 1
    ) as tvl_complete
    inner join prices.day as p1
        on
            tvl_complete.day = p1.timestamp
            and p1.contract_address = token1
            and p1.blockchain = '{{blockchain}}'
    inner join prices.day as p2
        on
            tvl_complete.day = p2.timestamp
            and p2.contract_address = token2
            and p2.blockchain = '{{blockchain}}'
    where latest = 1
),

-- With this we can plot the lp token price (tvl/lp total supply) over time
final as (
    select
        tvl.day,
        tvl,
        total_lp,
        tvl / total_lp as lp_token_price
    from tvl
    inner join lp_total_supply as lp
        on tvl.day = lp.day
)

-- Compute current value of initial investment together with other relevant output columns
select
    day,
    tvl,
    total_lp,
    lp_token_price,
    (
        -- Assess initial investment in lp tokens
        select 10000 / lp_token_price as investment
        from final
        where day = timestamp '{{start}}'
    ) * lp_token_price as current_value_of_investment
from final
where day >= timestamp '{{start}}'
order by 1 desc
