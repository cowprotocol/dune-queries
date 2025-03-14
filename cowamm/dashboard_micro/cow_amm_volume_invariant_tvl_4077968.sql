-- plot the tvl, surplus and volume of a pool
-- Parameters:
--   cow_amm: the address of the pool
--   start: the start date
with volume as (
    select
        block_date,
        sum(usd_value) as volume
    from cow_protocol_{{blockchain}}.trades
    where
        block_date >= timestamp '{{start}}'
        and trader = {{cow_amm}}
    group by 1
)

select
    day,
    volume,
    value0 + value1 as tvl,
    -- compute the invariant growth per LP token
    (
        power(reserve0, cast(weight0 as double) / 100) * power(reserve1, cast(weight1 as double) / 100) / lp_reserve
        - lag(power(reserve0, cast(weight0 as double) / 100) * power(reserve1, cast(weight1 as double) / 100) / lp_reserve) over (order by day)
    )
    -- then get the total invariant growth and price it in $$ and weight it with the same as the pool
    * lp_reserve * power(price0 * power(10, -decimals0), cast(weight0 as double) / 100) * power(price1 * power(10, -decimals1), cast(weight1 as double) / 100) as surplus
from dune.cowprotocol.result_amm_lp_infos
left join volume
    on
        day = block_date
where
    day >= timestamp '{{start}}'
    and contract_address = {{cow_amm}}
