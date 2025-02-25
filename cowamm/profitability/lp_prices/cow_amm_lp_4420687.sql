-- This query gets all the transfers during a day for all existing CoW AMMs:
-- Reserve tokens and lp tokens and the beginning of every day
--Parameters
--  {{blockchain}}: The blockchain to query
--  {{start}}: The start date of the analysis
--  {{end}}: The end date of the analysis. date(Timestamp) <= date(timestamp '{{end}}').
--      For a 1 day period, {{end}} = {{start}}
-- Hard coded start for the events scan to '2024-07-01', month of the Balancer AMMs launch

with date_range as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '{{start}}'),
            date(timestamp '{{end}}')
        )) t (day) --noqa: AL01
),

--Use the materialized view of query_3959044
--This is necessary because of performance issues.
--For large time frames the query is slow because of multiple accesses to logs
--The query has to run after the materialization.
cow_amm_pool as (
    select
        created_at,
        token_1_address as token0,
        token_2_address as token1,
        address as contract_address
    from dune.cowprotocol.result_balancer_co_w_am_ms
    where blockchain = '{{blockchain}}'
),

lp_balance_delta as (
    select
        p.contract_address,
        date(evt_block_time) as "day",
        sum(case when "from" = 0x0000000000000000000000000000000000000000 then cast(value as int256) else -cast(value as int256) end) as lp_transfer
    from cow_amm_pool as p
    left join erc20_{{blockchain}}.evt_transfer as t
        on p.contract_address = t.contract_address
    where
        ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
        and evt_block_time >= timestamp '2024-07-01'
    group by 1, 2
),

reserves_delta as (
    select
        p.contract_address,
        t.contract_address as token,
        date(t.evt_block_time) as "day",
        sum(case when t."from" = p.contract_address then -cast(t.value as int256) else cast(t.value as int256) end) as transfer
    from cow_amm_pool as p
    left join erc20_{{blockchain}}.evt_transfer as t
        on
            (
                t."from" = p.contract_address
                or t.to = p.contract_address
            )
    where
        t.contract_address in (p.token0, p.token1)
        -- transfers can only happen after the creation of the pool which happens after the launch
        and evt_block_time >= timestamp '2024-07-01'
    group by 1, 2, 3
)

select * from(
    select
        p.contract_address,
        p.created_at,
        token0,
        token1,
        d.day,
        price0.price as price0,
        price1.price as price1,
        price0.decimals as decimals0,
        price1.decimals as decimals1,
        sum(coalesce(l.lp_transfer, 0)) over (
            partition by p.contract_address
            order by d.day asc
            rows between unbounded preceding and 1 preceding
            ) as lp_reserve,
        sum(coalesce(r0.transfer, 0)) over (
            partition by p.contract_address
            order by d.day asc
            rows between unbounded preceding and 1 preceding
        ) as reserve0,
        sum(coalesce(r1.transfer, 0)) over (
            partition by p.contract_address
            order by d.day asc
            rows between unbounded preceding and 1 preceding
        ) as reserve1
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
)
where day >= created_at
