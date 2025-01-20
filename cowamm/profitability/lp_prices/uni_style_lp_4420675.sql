-- This query gets the reserve of every Uni style pool at the end of a day 
-- and the lp token transfers
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

pools as (
    select *
    from "query_4420646(blockchain='{{blockchain}}')"
),

lp_balance_delta as (
    select
        contract_address,
        date(evt_block_time) as "day",
        sum(case when "from" = 0x0000000000000000000000000000000000000000 then value else -value end) as lp_transfer
    from erc20_{{blockchain}}.evt_transfer
    where
        contract_address in (select contract_address from pools)
        and ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
        and evt_block_time >= date(timestamp '{{start}}')
        and date(evt_block_time) <= date(timestamp '{{end}}')
    group by 1, 2
),

syncs as (
    select
        pools.contract_address,
        date_trunc('day', block_time) as "day",
        varbinary_to_uint256(substr(data, 1, 32)) as reserve0,
        varbinary_to_uint256(substr(data, 33, 32)) as reserve1,
        rank() over (partition by (pools.contract_address, date_trunc('day', block_time)) order by block_time desc, index desc) as latest
    from {{blockchain}}.logs
    inner join pools
        on logs.contract_address = pools.contract_address
    where
        topic0 = 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1 -- Sync
        and logs.block_time >= date(timestamp '{{start}}')
        and date(logs.block_time) <= date(timestamp '{{end}}')
)

select
    p.contract_address,
    project,
    token0,
    token1,
    d.day,
    price0.price as price0,
    price1.price as price1,
    price0.decimals as decimals0,
    price1.decimals as decimals1,
    coalesce(l.lp_transfer, 0) as lp_transfer,
    --fill in the days without reserves
    last_value(s.reserve0) ignore nulls over (partition by p.contract_address order by d.day range between unbounded preceding and current row) as reserve0,
    last_value(s.reserve1) ignore nulls over (partition by p.contract_address order by d.day range between unbounded preceding and current row) as reserve1
from date_range as d
cross join pools as p
left join lp_balance_delta as l
    on
        d.day = l.day
        and p.contract_address = l.contract_address
--only keep the last reserves of the day
left join (--noqa: ST05
    select *
    from syncs
    where latest = 1
) as s
    on
        d.day = s.day
        and p.contract_address = s.contract_address
left join prices.day as price0
    on
        d.day = price0.timestamp
        and p.token0 = price0.contract_address
left join prices.day as price1
    on
        d.day = price1.timestamp
        and p.token1 = price1.contract_address
where
    (price0.blockchain = '{{blockchain}}' or price0.blockchain is null)
    and (price1.blockchain = '{{blockchain}}' or price0.blockchain is null)
    and d.day >= date_trunc('day', p.created_at)
