-- Computes the balances and their dollar value of a all CoW AMM pools for a specific token pair at every transaction where it changed
-- Parameters
--  {{token_a}} - either token of the pool
--  {{token_b}} - other token of the pool

with cow_amm_pool as (
    select
        created_at,
        address
    from query_3959044
    where ((token_1_address = {{token_a}} and token_2_address = {{token_b}}) or (token_2_address = {{token_a}} and token_1_address = {{token_b}}))
    order by 1 desc
),

-- Computes the token balance changes of the relevant token per transaction
reserves_delta as (
    select
        evt_block_time,
        evt_tx_hash,
        p.address as pool,
        contract_address as token,
        MAX(evt_block_number) as evt_block_number,
        MAX(evt_index) as evt_index,
        SUM(case when "from" = p.address then -value else value end) as amount
    from erc20_ethereum.evt_transfer as t
    inner join cow_amm_pool as p
        on
            t."from" = p.address
            or t.to = p.address
    where contract_address in ({{token_a}}, {{token_b}})
    group by 1, 2, 3, 4
),

-- sums token balance changes to get total balances of relevant tokens per transaction
balances_by_tx as (
    select
        evt_block_time,
        evt_tx_hash,
        pool,
        token,
        SUM(amount) over (partition by (pool, token) order by (evt_block_number, evt_index)) as balance
    from reserves_delta
),

-- joins token balances with prices to get tvl of pool per transaction
tvl as (
    select
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        b.pool,
        b.token,
        balance,
        price,
        balance * price / POW(10, decimals) as tvl
    from balances_by_tx as b
    inner join prices.usd as p
        on
            p.minute = DATE_TRUNC('minute', evt_block_time)
            and b.token = p.contract_address
)

select
    tvl1.block_time,
    tvl1.tx_hash,
    tvl1.pool,
    tvl1.token as token1,
    tvl1.balance as balance1,
    tvl1.price as price1,
    tvl2.token as token2,
    tvl2.balance as balance2,
    tvl2.price as price2,
    tvl1.tvl + tvl2.tvl as tvl
from tvl as tvl1
inner join tvl as tvl2
    on
        tvl1.tx_hash = tvl2.tx_hash
        and tvl1.pool = tvl2.pool
        and tvl1.token > tvl2.token
