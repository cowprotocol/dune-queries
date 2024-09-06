-- This query computes Coincidence of Wants fractions on CoW Protocol for individual tokens
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- It computes a CoW fraction per batch by averaging the CoW fractions from query 4021555 weighted by usd volume
--
-- The query is based on a Master's Thesis by Vigan Lladrovci
-- https://wwwmatthes.in.tum.de/pages/y9xcjv094zhn/Master-s-Thesis-Vigan-Lladrovci

with cow_per_token as (
    select * from "query_4021555(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

-- get prices from trades table
token_prices as (
    select * from "query_4031637(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

-- join token amounts with prices from trades table
cow_per_token_with_prices as (
    select
        cpt.*,
        token_price
    from cow_per_token as cpt
    left outer join token_prices as tp
        on cpt.tx_hash = tp.tx_hash and cpt.token_address = tp.token_address
),

-- convert amounts to dollar values
cow_per_token_usd as (
    select
        block_time,
        tx_hash,
        token_address,
        naive_cow_potential,
        naive_cow,
        token_price * user_in as user_in,
        token_price * user_out as user_out,
        token_price * amm_in as amm_in,
        token_price * amm_out as amm_out,
        token_price * slippage_in as slippage_in,
        token_price * slippage_out as slippage_out
    from cow_per_token_with_prices
),

-- aggregate token volumes
cow_volume_per_batch as (
    select
        block_time,
        tx_hash,
        sum(user_in) as user_in,
        sum(user_out) as user_out,
        sum(amm_in) as amm_in,
        sum(amm_out) as amm_out,
        sum(slippage_in) as slippage_in,
        sum(slippage_out) as slippage_out,
        sum(user_out * naive_cow_potential) as naive_cow_potential_volume,
        sum(user_out * naive_cow) as naive_cow_volume
    from cow_per_token_usd
    group by block_time, tx_hash
)

-- compute cow values per batch
select
    *,
    case when user_out > 0 then naive_cow_potential_volume / user_out end as naive_cow_potential,
    case when user_out > 0 then naive_cow_volume / user_out end as naive_cow
from cow_volume_per_batch
