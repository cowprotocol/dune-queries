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
        date(coalesce(t0.evt_block_time, t1.evt_block_time)) as "day",
        sum(case when t0."from" = p.contract_address then -t0.value else t0.value end) as transfer0,
        sum(case when t1."from" = p.contract_address then -t1.value else t1.value end) as transfer1
    from cow_amm_pool as p
    left join erc20_{{blockchain}}.evt_transfer as t0
        on
            (
                t0."from" = p.contract_address
                or t0.to = p.contract_address
            )
            and t0.contract_address = p.token0
    left join erc20_{{blockchain}}.evt_transfer as t1
        on
            (
                t1."from" = p.contract_address
                or t1.to = p.contract_address
            )
            and t1.contract_address = p.token1
    where
        t0.evt_block_time >= date(timestamp '{{start}}')
        and date(t0.evt_block_time) <= date(timestamp '{{end}}')
        and t1.evt_block_time >= date(timestamp '{{start}}')
        and date(t1.evt_block_time) <= date(timestamp '{{end}}')
    group by 1, 2
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
    coalesce(r.transfer0, 0) as transfer0,
    coalesce(r.transfer1, 0) as transfer1
from date_range as d
cross join cow_amm_pool as p
left join lp_balance_delta as l
    on
        d.day = l.day
        and p.contract_address = l.contract_address
left join reserves_delta as r
    on
        d.day = r.day
        and p.contract_address = r.contract_address
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
    and coalesce(price0.blockchain, '{{blockchain}}') = '{{blockchain}}'
