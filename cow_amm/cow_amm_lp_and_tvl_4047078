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
            date(timestamp '{{start}}') - INTERVAL '1' day,
            date(now())
        )) t (day) --noqa: AL01
),

-- Finds the CoW AMM pool address given tokens specified in query parameters (regardless of order) 
cow_amm_pool as (
select blockchain, address, token_1_address, token_2_address
from query_3959044 q
where ((token_1_address = {{token_a}} and token_2_address = {{token_b}}) or (token_2_address = {{token_a}} and token_1_address = {{token_b}}))
)

-- per day lp token total supply changes of the CoW AMM pool by looking at burn/mint events
, get_lp_balance as (
    select date(evt_block_time) as day, 
    sum(case when "from" = 0x0000000000000000000000000000000000000000 THEN (value/1e18) ELSE -(value/1e18) END) as lp_supply
    from erc20_ethereum.evt_transfer
    where contract_address in (select address from cow_amm_pool)
    and ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
    group by 1
)

, total_lp as (
select day, lp_supply, sum(lp_supply) over (order by day) as total_lp
from get_lp_balance
)

-- lp token total supply without date gaps
 , lp_total_supply as (
SELECT
        date_range.day,
        COALESCE(v.total_lp, LAG(v.total_lp) OVER (ORDER BY date_range.day)) AS total_lp
    FROM
        date_range
    LEFT JOIN
        total_lp v ON date_range.day = v.day
)
-- Compute tvl by multiplying each day's closing price with bet transfers in and out of the pool
, get_tvl as (
SELECT x.day, SUM(amount * price_close) as total_tvl FROM (
    SELECT date(evt_block_time) as day, 
            symbol, 
            -(value/POW(10,decimals)) as amount
    FROM erc20_ethereum.evt_transfer a LEFT JOIN tokens.erc20 b ON a.contract_address = b.contract_address and blockchain in (select blockchain from cow_amm_pool)
    where "from"  in (select address from cow_amm_pool)
        and (a.contract_address IN (select token_1_address from cow_amm_pool) or 
        a.contract_address IN (select token_2_address from cow_amm_pool))
      
    UNION ALL
    SELECT date(evt_block_time) as day, 
            symbol, 
            (value/POW(10,decimals)) as amount
    FROM erc20_ethereum.evt_transfer a LEFT JOIN tokens.erc20 b ON a.contract_address = b.contract_address and blockchain in (select blockchain from cow_amm_pool)
    where "to"  in (select address from cow_amm_pool)
        and (a.contract_address IN (select token_1_address from cow_amm_pool) or 
        a.contract_address IN (select token_2_address from cow_amm_pool))
) x join prices.usd_daily y
    ON blockchain in (select blockchain from cow_amm_pool) and x.symbol = y.symbol and y.day=x.day
group by 1
)

, total_tvl as (
select day, sum(total_tvl) over (order by day) as total_tvl
from get_tvl
)

-- With this we can plot the lp token price (tvl/lp total supply) over time
, final as (
  select 
  t1.day, 
  total_tvl as tvl, 
  total_lp, 
  total_tvl/total_lp as lp_token_price
from total_tvl t1
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
        select 10000 * lp_token_price as investment
        from final
        where day = timestamp '{{start}}'
    ) / lp_token_price as current_value_of_investment
from final
where day >= timestamp '{{start}}'
order by 1 desc

