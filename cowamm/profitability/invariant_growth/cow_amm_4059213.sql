-- Computes the surplus per $100 tvl per day (aka its invariant growth) for a CoW AMM.
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run
--  {{blockchain} - chain for which the query is running

-- Finds the CoW AMM pool address given tokens specified in query parameters (regardless of order) 
with cow_amm_pool as (
    select
        created_at,
        address
    from query_3959044
    where ((token_1_address = {{token_a}} and token_2_address = {{token_b}}) or (token_2_address = {{token_a}} and token_1_address = {{token_b}}))
    order by 1 desc
    limit 1
)

-- computes, surplus, tvl and thus relative surplus (per $100)
select
    block_date as "day",
    SUM(usd_value) as volume,
    SUM(surplus_usd) as absolute_invariant_growth,
    AVG(tvl) as tvl,
    SUM(surplus_usd / tvl) as pct_invariant_growth
from cow_protocol_{{blockchain}}.trades as t
inner join "query_4059700(token_a='{{token_a}}', token_b='{{token_b}}', blockchain='{{blockchain}}')" as tvl
    on
        t.tx_hash = tvl.tx_hash
        and tvl.pool = trader
        and trader = (select address from cow_amm_pool)
group by 1
