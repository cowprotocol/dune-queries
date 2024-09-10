-- Computes the TVL, lp token total supply and lp token price of a CoW AMM pool over time
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run

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
    select distinct
        cowamm_address as address,
        blockchain,
        token_1_address,
        token_2_address
    from (
        select
            t.blockchain,
            cowamm_address,
            token_1_reserve as token_1_balance,
            token_2_reserve as token_2_balance,
            t.token_1_address,
            t.token_2_address,
            count(coalesce(token_1_reserve, token_2_reserve)) over (partition by cow_amm_nb order by "time" desc) as "temp"
        from query_3959058 as t
        left join
            (select
                *,
                count(address) over (order by address, token_1_address, token_2_address) as cow_amm_nb
            from query_3959044) as p
            on
                t.cowamm_address = p.address and t.blockchain = p.blockchain
                and t.token_1_address = p.token_1_address and t.token_2_address = p.token_2_address

        where
            ((t.token_1_address = {{token_a}} and t.token_2_address = {{token_b}}) or (t.token_2_address = {{token_a}} and t.token_1_address = {{token_b}}))
    ) where temp = 1
    and token_1_balance > 0 and token_2_balance > 0
),

-- per day lp token total supply changes of the CoW AMM pool by looking at burn/mint events
lp_balance_delta as (
    select
        date(evt_block_time) as "day",
        sum(case when "from" = 0x0000000000000000000000000000000000000000 then value else -value end) as lp_supply
    from erc20_ethereum.evt_transfer
    where
        contract_address in (select address from cow_amm_pool)
        and ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
    group by 1
),

total_lp as (
    select
        day,
        lp_supply,
        sum(lp_supply) over (order by day) as total_lp
    from lp_balance_delta
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
        inner join total_lp as lp
            on date_range.day >= lp.day
            -- performance optimisation: this assumes one week prior to start there was at least one lp supply change event
            and lp.day >= (timestamp '{{start}}' - interval '7' day)
    )
    where latest = 1
),

-- Compute tvl by multiplying each day's closing price with net transfers in and out of the pool
get_tvl as (
    select
        x.day,
        sum(amount * price_close / pow(10, decimals)) as total_tvl
    from (
        select
            date(evt_block_time) as "day",
            contract_address,
            -value as amount
        from erc20_ethereum.evt_transfer
        where
            "from" in (select address from cow_amm_pool)
            and contract_address in ({{token_a}}, {{token_b}})

        union all
        select
            date(evt_block_time) as "day",
            contract_address,
            value as amount
        from erc20_ethereum.evt_transfer
        where
            "to" in (select address from cow_amm_pool)
            and contract_address in ({{token_a}}, {{token_b}})
    ) as x inner join prices.usd_daily as y
        on blockchain = 'ethereum' and x.day = y.day and x.contract_address = y.contract_address
    group by 1
),

total_tvl_prep as (
    select
        day,
        sum(total_tvl) over (order by day) as total_tvl
    from get_tvl
),

total_tvl as (
    select
        day,
        total_tvl
    from (
        -- join full date range with potentially incomplete data. This results in many rows per day (all total tvl on or before that day)
        -- rank() is then used to order join candidates by recency (rank = 1 is the latest tvl)
        select
            date_range.day,
            total_tvl,
            rank() over (partition by (date_range.day) order by tvl.day desc) as latest
        from date_range
        inner join total_tvl_prep as tvl
            on date_range.day >= tvl.day
            -- performance optimisation: this assumes one week prior to start there was at least one tvl change event
            and tvl.day >= (timestamp '{{start}}' - interval '7' day)
    )
    where latest = 1
),

-- With this we can plot the lp token price (tvl/lp total supply) over time
final as (
    select
        t1.day,
        total_tvl as tvl,
        total_lp,
        total_tvl / total_lp as lp_token_price
    from total_tvl as t1
    inner join lp_total_supply as lp
        on t1.day = lp.day
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
