-- Issues with the other version (4420687)
-- Trying by accessing only once the logs table to get assets reserve
-- However for periods longer than 1 month, the query exceeds cluster capacity
-- The issue is with the join between both reserves_delta tables
-- Although this table is only 4000 rows at most (~10kb)

--Parameters
--  {{blockchain}}: The blockchain to query
--  {{start}}: The start date of the analysis
--  {{end}}: The end date of the analysis. date(Timestamp) <= date(timestamp '{{end}}').
--      For a 1 day period, {{end}} = {{start}}

with date_range as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '{{start}}'),
            date(timestamp '{{end}}')
        )) t (day) --noqa: AL01
),

--use the materialized view of query_3959044
cow_amm_pool as (
    select
        created_at,
        token_1_address as token0,
        token_2_address as token1,
        address as contract_address
    from query_3959044
    where blockchain = '{{blockchain}}'
),

lp_balance_delta as (
    select
        contract_address,
        date(evt_block_time) as "day",
        sum(case when "from" = 0x0000000000000000000000000000000000000000 then value else -value end) as lp_transfer
    from erc20_{{blockchain}}.evt_transfer
    where
        contract_address in (select contract_address from cow_amm_pool)
        and ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
        and evt_block_time >= date(timestamp '{{start}}')
        and date(evt_block_time) <= date(timestamp '{{end}}')
    group by 1, 2
),

reserves_delta as (
    select
        p.contract_address,
        t.contract_address as token,
        date(t.evt_block_time) as "day",
        sum(case when t."from" = p.contract_address then -t.value else t.value end) as transfer
    from cow_amm_pool as p
    left join erc20_{{blockchain}}.evt_transfer as t
        on
            (
                t."from" = p.contract_address
                or t.to = p.contract_address
            )
    where
        t.evt_block_time >= date(timestamp '{{start}}')
        and date(t.evt_block_time) <= date(timestamp '{{end}}')
        and t.contract_address in (p.token0, p.token1)
    group by 1, 2, 3
)

select
    p.contract_address,
    token0,
    token1,
    d.day,
    price0.price as price0,
    price1.price as price1,
    price0.decimals as decimals0,
    price1.decimals as decimals1,
    coalesce(l.lp_transfer, 0) as lp_transfer,
    coalesce(r0.transfer, 0) as transfer0,
    coalesce(r1.transfer, 0) as transfer1
from date_range as d
cross join cow_amm_pool as p
left join lp_balance_delta as l
    on
        d.day = l.day
        and p.contract_address = l.contract_address
left join reserves_delta as r0
    on
        d.day = r0.day
        and p.contract_address = r0.contract_address
        and p.token0 = r0.token
left join reserves_delta as r1
    on
        d.day = r1.day
        and p.contract_address = r1.contract_address
        and p.token1 = r1.token
left join prices.day as price0
    on
        d.day = price0.timestamp
        and p.token0 = price0.contract_address
left join prices.day as price1
    on
        d.day = price1.timestamp
        and p.token1 = price1.contract_address
where
    coalesce(price0.blockchain, '{{blockchain}}') = '{{blockchain}}'
    and coalesce(price1.blockchain, '{{blockchain}}') = '{{blockchain}}'
