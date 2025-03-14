-- plot the tvl, surplus and volume of a pool
-- Parameters:
--   cow_amm: the address of the pool
--   start: the start date
with volume as (
    select
        block_date,
        sum(usd_value) as volume
    from cow_protocol_ethereum.trades
    where
        block_date >= timestamp '{{start}}'
        and trader = {{cow_amm}}
    group by 1)

select
    day,
    tvl,
    volume,
    -- compute the invariant growth per LP token
    (power(value0, weight0) * power(value1, weight1) / lp_reserve
    - lag(power(reserve0, weight0) * power(reserve1, weight1) / lp_reserve) over (order by day))
    -- then get the total invariant growth and price it in $$
    * lp_reserve * price0 * power(10, -decimals0 * weight0) * price1 * power(10, -decimals1 * weight1) as surplus
from dune.cowprotocol.result_amm_lp_infos
inner join volume
    on
        day = block_date
        and contract_address = {{cow_amm}}
where day >= timestamp '{{start}}'
