-- This query gets the token reserves (token0, 1, lp) of every Univ2 style pool with a CoW AMM version at the beginning of every day 
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
uni_style_pools as (
    select
        u.created_at,
        u.contract_address,
        u.token0,
        u.token1,
        u.project,
        --cow amms can have multiple pools for a same pair
        min(c.created_at) as cow_created_at
    from "query_4420646(blockchain = '{{blockchain}}')" as u
    inner join cow_amm_pools as c
        on
            (
                (u.token0 = c.token_0_address and u.token1 = c.token_1_address)
                or (u.token1 = c.token_0_address and u.token0 = c.token_1_address)
            )
            and '{{blockchain}}' = c.blockchain
    group by 1, 2, 3, 4, 5
),

--time frame of the analysis
date_range as (
    select t.day
    from
        unnest(sequence(
            date(timestamp '{{start}}'),
            least(date(timestamp '{{end}}'), date(now()))
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
        and date(evt_block_time) <= least(date(timestamp '{{end}}'), date(now()))
    group by 1, 2
),

lp_reserve_first as (
    select
        contract_address,
        sum(lp_transfer) as lp_reserve_first
    from lp_balance_delta
    where day < date(timestamp '{{start}}')
    group by contract_address --noqa: AM06
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
        and date(logs.block_time) <= least(date(timestamp '{{end}}'), date(now()))
),

syncs_first as (
    select *
    from (
        select
            contract_address,
            reserve0,
            reserve1,
            rank() over (partition by contract_address order by day desc) as latest_first
        from syncs
        where
            day < date(timestamp '{{start}}')
            and latest = 1
    )
    where latest_first = 1
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
        price0.symbol as symbol0,
        price1.symbol as symbol1,
        u.project,
        price0.price as price0,
        price1.price as price1,
        price0.decimals as decimals0,
        price1.decimals as decimals1,
        coalesce(lrf.lp_reserve_first, 0) + coalesce(sum(coalesce(l.lp_transfer, 0)) over (
            partition by u.contract_address
            order by d.day asc
            rows between unbounded preceding and 1 preceding
        ), 0) as lp_reserve,
        coalesce(
            last_value(s.reserve0) over (
                partition by s.contract_address
                order by s.day asc
                rows between unbounded preceding and 1 preceding
            ),
            sf.reserve0, 0
        ) as reserve0,
        coalesce(
            last_value(s.reserve1) over (
                partition by s.contract_address
                order by s.day asc
                rows between unbounded preceding and 1 preceding
            ),
            sf.reserve1, 0
        ) as reserve1
    from uni_style_pools as u
    cross join date_range as d
    left join lp_balance_delta as l
        on
            u.contract_address = l.contract_address
            and d.day = l.day
    left join lp_reserve_first as lrf
        on u.contract_address = lrf.contract_address
    left join (select * from syncs where latest = 1) as s
        on
            u.contract_address = s.contract_address
            and d.day = s.day
    left join syncs_first as sf
        on u.contract_address = sf.contract_address
    left join (select distinct * from prices.day) as price0
        on
            d.day = price0.timestamp
            and u.token0 = price0.contract_address
    left join (select distinct * from prices.day) as price1
        on
            d.day = price1.timestamp
            and u.token1 = price1.contract_address
    where
        coalesce(price0.blockchain, '{{blockchain}}') = '{{blockchain}}'
        and coalesce(price1.blockchain, '{{blockchain}}') = '{{blockchain}}'
)
where day >= cow_created_at
