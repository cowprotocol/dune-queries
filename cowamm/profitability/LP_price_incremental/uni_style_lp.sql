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
    select
        substr(data, 13, 20) as contract_address,
        substr(topic1, 13, 20) as token0,
        substr(topic2, 13, 20) as token1,
        case
            when
                contract_address in (
                    0x1097053Fd2ea711dad45caCcc45EfF7548fCB362,
                    0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e
                ) then 'pancakeswap'
            when
                contract_address in (
                    0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f,
                    0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9,
                    0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6
                )
                then 'uniswap'
            when
                contract_address in (
                    0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac,
                    0xc35DADB65012eC5796536bD9864eD8773aBc74C4,
                    0x71524B4f93c58fcbF659783284E38825f0622859
                )
                then 'sushiswap'
        end as project
    from {{blockchain}}.logs
    where
        topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
        and contract_address in
        (
            0x1097053Fd2ea711dad45caCcc45EfF7548fCB362, --eth, pancake
            0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e, --arb/bas, pancake
            0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f, --eth, uni
            0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9, --arb, uni
            0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6, --bas, uni
            0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac, --eth, sushi
            0xc35DADB65012eC5796536bD9864eD8773aBc74C4, --arb/gno, sushi
            0x71524B4f93c58fcbF659783284E38825f0622859 --bas, sushi
        )
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
        rank() over (partition by (pools.contract_address) order by block_time desc, index desc) as latest
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
    coalesce(s.reserve0, 0) as reserve0,
    coalesce(s.reserve1, 0) as reserve1
from date_range as d
cross join pools as p
left join lp_balance_delta as l
    on
        d.day = l.day
        and p.contract_address = l.contract_address
left join syncs as s
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
    coalesce(price0.blockchain, '{{blockchain}}') = '{{blockchain}}'
    and coalesce(price0.blockchain, '{{blockchain}}') = '{{blockchain}}'
    and s.latest = 1
