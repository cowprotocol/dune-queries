-- Computes the TVL, lp token total supply and lp token price of a uniswap pool over time
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

-- Finds the uniswap v2 pool address given tokens specified in query parameters (regardless of order)
pool as (
    select
        pool as contract_address,
        token0,
        token1
    from uniswap_ethereum.pools
    where
        ((token0 = {{token_a}} and token1 = {{token_b}}) or (token1 = {{token_a}} and token0 = {{token_b}}))
        and version = 'v2'
    limit 1
),

-- per day lp token total supply changes of the uniswap pool by looking at burn/mint events
lp_supply_delta as (
    select
        date(evt_block_time) as "day",
        sum(case when "from" = 0x0000000000000000000000000000000000000000 then value else -value end) as delta
    from erc20_ethereum.evt_transfer
    where
        contract_address = (select contract_address from pool)
        and ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
    group by 1
),

-- per day lp token total supply by summing up all deltas (may have missing records for some days)
lp_total_supply_incomplete as (
    select
        day,
        sum(delta) over (order by day) as total_lp
    from lp_supply_delta
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
            and lp.day >= (timestamp '{{start}}' - interval '7' day)
    )
    where latest = 1
),

-- TVL calculation is based on `Sync` events which are emitted on every swap and contain current reserve amounts.
-- Get all sync events that happened on or before the end of each day in our date range and rank them by recency
-- the entry with rank 1 is the latest sync event before a target day (allowing for days without any sync).
latest_syncs_per_day as (
    select
        day,
        pool.contract_address,
        token0,
        reserve0,
        token1,
        reserve1,
        rank() over (partition by (date_range.day) order by (evt_block_number, evt_index) desc) as latest
    from uniswap_v2_ethereum.Pair_evt_Sync as sync
    inner join date_range
        on day >= date(evt_block_time)
    inner join pool
        on sync.contract_address = pool.contract_address
),

-- Get reserve balances of token_a and token_b per day, by looking at the last `Sync` emitted event for each day
reserve_balances as (
    select
        date_range.day,
        contract_address,
        token0,
        reserve0,
        token1,
        reserve1
    from date_range
    left join latest_syncs_per_day
        on
            date_range.day = latest_syncs_per_day.day
            and latest = 1
),

-- Compute tvl by multiplying each day's closing price with balance
tvl as (
    select
        balances.day,
        balances.contract_address,
        sum(balance * price_close / pow(10, decimals)) as tvl
    from (
        -- turns (date, balance0, balance1) into (date, balance0) + (date, balance1)
        select
            day,
            contract_address,
            reserve0 as balance,
            token0 as token
        from reserve_balances
        union distinct
        select
            day,
            contract_address,
            reserve1 as balance,
            token1 as token
        from reserve_balances
    ) as balances
    left join prices.usd_daily as prices
        on
            blockchain = 'ethereum'
            and balances.token = prices.contract_address
            and balances.day = prices.day
    group by 1, 2
),

-- With this we can plot the lp token price (tvl/lp total supply) over time
lp_token_price as (
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
        from lp_token_price
        where day = timestamp '{{start}}'
    ) * lp_token_price as current_value_of_investment
from lp_token_price
order by 1 desc
