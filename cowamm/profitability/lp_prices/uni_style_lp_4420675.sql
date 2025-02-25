-- This query gets the reserve of every Univ2 style pool at the beginning of every day 
-- and the lp token transfers
--Parameters
--  {{blockchain}}: The blockchain to query
--  {{start}}: The start date of the analysis
--  {{end}}: The end date of the analysis. date(Timestamp) <= date(timestamp '{{end}}').
--      For a 1 day period, {{end}} = {{start}}

--

with cow_amm_pools as (
    select * from dune.cowprotocol.result_balancer_co_w_am_ms
),

--get all the the pools derived from uniswap which compare to cow amms
uni_style_pools as(
    select
        u.created_at,
        u.contract_address,
        u.token0,
        u.token1,
        u.project,
        --cow amms can have multiple pools for a same pair
        min(c.created_at) as cow_created_at
    from "query_4420646(blockchain = 'ethereum')" as u
    inner join cow_amm_pools as c
        on 
            (u.token0 = c.token_1_address and u.token1 = c.token_2_address)
            or (u.token1 = c.token_1_address and u.token0 = c.token_2_address)
    group by 1, 2, 3, 4, 5
),

--time frame of the analysis
date_range as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '{{start}}'),
            date(timestamp '{{end}}')
        )) t (day) --noqa: AL01
),

lp_balance_delta as (
    select
        evt_transfer.contract_address,
        date(evt_block_time) as "day",
        sum(case when "from" = 0x0000000000000000000000000000000000000000 then value else -value end) as lp_transfer
    from erc20_{{blockchain}}.evt_transfer
    inner join uni_style_pools as u
        on u.contract_address = evt_transfer.contract_address
    where
        ("from" = 0x0000000000000000000000000000000000000000 or "to" = 0x0000000000000000000000000000000000000000)
        and evt_block_time >= date(u.created_at)
    group by 1, 2
),

syncs as (
    select
        u.contract_address,
        date_trunc('day', block_time) as "day",
        varbinary_to_uint256(substr(data, 1, 32)) as reserve0,
        varbinary_to_uint256(substr(data, 33, 32)) as reserve1,
        rank() over (partition by (logs.contract_address, date_trunc('day', block_time)) order by block_time desc, index desc) as latest
    from {{blockchain}}.logs
    inner join uni_style_pools as u
        on logs.contract_address = u.contract_address
    where
        topic0 = 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1 -- Sync
        and logs.block_time >= date(u.created_at)
)

select *
from (
    select 
        u.created_at,
        u.cow_created_at,
        u.contract_address,
        d.day,
        u.token0,
        u.token1,
        u.project,
        sum(coalesce(l.lp_transfer, 0)) over (
            partition by u.contract_address
            order by d.day asc
            rows between unbounded preceding and 1 preceding
        ) as lp_reserve,
        coalesce(
            last_value(s.reserve0) over (
                partition by s.contract_address 
                order by s.day asc 
                rows between unbounded preceding and 1 preceding),
            0
        ) as reserve0,
        coalesce(
            last_value(s.reserve1) over (
                partition by s.contract_address 
                order by s.day asc 
                rows between unbounded preceding and 1 preceding),
            0
        ) as reserve1
    from uni_style_pools as u
    cross join date_range as d
    left join lp_balance_delta as l
        on 
            u.contract_address = l.contract_address
            and d.day = l.day
    left join (select * from syncs where latest = 1) as s
        on 
            u.contract_address = s.contract_address
            and d.day = s.day
)
where day >= created_at