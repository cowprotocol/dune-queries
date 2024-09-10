-- Computes the absolute surplus per $100 tvl per day (aka its invariant growth) for a CoW AMM.
-- Parameters
--  {{token_a}} - either token of the desired uni pool
--  {{token_b}} - other token of the desired uni pool
--  {{start}} - date as of which the analysis should run

-- Finds the CoW AMM pool address given tokens specified in query parameters (regardless of order) 
with cow_amm_pool as (
    select
        block_time,
        address,
        token_1_address,
        token_2_address
    from query_3959044
    where ((token_1_address = {{token_a}} and token_2_address = {{token_b}}) or (token_2_address = {{token_a}} and token_1_address = {{token_b}}))
    order by 1 desc
    limit 1
),

-- Computes the token balance changes of the relevant token per transaction
reserves_delta as (
    select
        evt_block_time,
        evt_tx_hash,
        contract_address,
        MAX(evt_block_number) as evt_block_number,
        MAX(evt_index) as evt_index,
        SUM(-value) as amount
    from erc20_ethereum.evt_transfer
    where
        "from" in (select address from cow_amm_pool)
        and contract_address in ({{token_a}}, {{token_b}})
    group by 1, 2, 3
    union all
    select
        evt_block_time,
        evt_tx_hash,
        contract_address,
        MAX(evt_block_number) as evt_block_number,
        MAX(evt_index) as evt_index,
        SUM(value) as amount
    from erc20_ethereum.evt_transfer
    where
        "to" in (select address from cow_amm_pool)
        and contract_address in ({{token_a}}, {{token_b}})
    group by 1, 2, 3
),

-- sums token balance changes to get total balances of relevant tokens per transaction
balances_by_tx as (
    select
        evt_block_time,
        evt_tx_hash,
        contract_address,
        SUM(amount) over (partition by contract_address order by (evt_block_number, evt_index)) as balance
    from reserves_delta
),

-- joins token balances with prices to get tvl of pool per transaction
tvl as (
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        b.contract_address,
        balance,
        price,
        balance * price / POW(10, decimals) as tvl
    from balances_by_tx as b
    inner join prices.usd as p
        on
            p.minute = DATE_TRUNC('minute', evt_block_time)
            and b.contract_address = p.contract_address
)

-- computes, surplus, tvl and thus relative surplus (per $100)
select
    block_date,
    SUM(surplus_usd) as absolute_invariant_growth,
    AVG(tvl1.tvl + tvl2.tvl) as tvl,
    SUM(surplus_usd / (tvl1.tvl + tvl2.tvl)) * 100 as pct_invariant_growth
from cow_protocol_ethereum.trades as t
inner join tvl as tvl1
    on
        t.tx_hash = tvl1.tx_hash
        and tvl1.contract_address = {{token_a}}
inner join tvl as tvl2
    on
        t.tx_hash = tvl2.tx_hash
        and tvl2.contract_address = {{token_b}}
where trader = (select address from cow_amm_pool)
group by 1
order by 1 desc
