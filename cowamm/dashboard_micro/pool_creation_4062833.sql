--display the informations about a cow amm pool
-- Parameters:
--   cow_amm: the address of the pool

select
    date(created_at) as created_day,
    concat('<a href="https://etherscan.io/address/', cast(contract_address as varchar), '" target="_blank">', cast(contract_address as varchar), '</a>') as pool_address,
    concat('<a href="https://etherscan.io/address/', cast(token0 as varchar), '" target="_blank">', symbol0, ' ', cast(weight0 as varchar), '%', '</a>') as symbol0,
    concat('<a href="https://etherscan.io/address/', cast(token1 as varchar), '" target="_blank">', symbol1, ' ', cast(weight1 as varchar), '%', '</a>') as symbol1
from dune.cowprotocol.result_amm_lp_infos
where (contract_address = {{cow_amm}})
order by 1 desc
limit 1
