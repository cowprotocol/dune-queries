--Groups all competitors in one query

-- Uniswap, PancakeSwap, Sushiwap
select
    contract_address,
    tvl,
    fee,
    volume,
    apr,
    project,
    case
        when '{{blockchain}}' = 'ethereum' then 1
        when '{{blockchain}}' = 'ethereum' then 100
        when '{{blockchain}}' = 'arbitrum' then 42161
    end as chain_id
from "query_4304295(blockchain='{{blockchain}}')"

union distinct

--Curve
select
    contract_address,
    tvl,
    fee,
    volume,
    apr,
    'curve' as project,
    case
        when '{{blockchain}}' = 'ethereum' then 1
        when '{{blockchain}}' = 'ethereum' then 100
        when '{{blockchain}}' = 'arbitrum' then 42161
    end as chain_id
from "query_4232873(blockchain='{{blockchain}}')"
