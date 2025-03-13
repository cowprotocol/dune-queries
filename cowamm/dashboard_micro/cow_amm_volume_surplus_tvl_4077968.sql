-- plot the tvl, surplus and volume of a pool
-- Parameters:
--   cow_amm: the address of the pool
--   start: the start date

select
    day,
    value0 + value1 as tvl,
    sum(surplus_usd) as surplus,
    sum(usd_value) as volume
from dune.cowprotocol.result_amm_lp_infos
inner join cow_protocol_ethereum.trades
    on
        trader = {{cow_amm}}
        and day = block_date
where
    day >= timestamp '{{start}}'
    and contract_address = {{cow_amm}}
group by 1, 2
