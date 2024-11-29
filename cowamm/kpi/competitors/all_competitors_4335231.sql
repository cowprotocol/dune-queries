--Groups all competitors in one query
-- Parameters:
-- {{blockchain}}: The blockchain to query

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
        when '{{blockchain}}' = 'gnosis' then 100
        when '{{blockchain}}' = 'base' then 8453
        when '{{blockchain}}' = 'arbitrum' then 42161
    end as chain_id
from "query_4304295(blockchain='{{blockchain}}', competitor_end_time='{{competitor_end_time}}')"

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
        when '{{blockchain}}' = 'gnosis' then 100
        when '{{blockchain}}' = 'base' then 8453
        when '{{blockchain}}' = 'arbitrum' then 42161
    end as chain_id
from "query_4232873(blockchain='ethereum', competitor_end_time='{{competitor_end_time}}')"
-- there are no significant curve pools on arbitrum/gnosis
where {{blockchain}} = 'ethereum'

union distinct

--CoW AMM
select
    contract_address,
    tvl,
    0 as fee,
    coalesce(volume, 0) as volume,
    coalesce(apr, 0) as apr,
    'CoW AMM' as project,
    case
        when '{{blockchain}}' = 'ethereum' then 1
        when '{{blockchain}}' = 'gnosis' then 100
        when '{{blockchain}}' = 'base' then 8453
        when '{{blockchain}}' = 'arbitrum' then 42161
    end as chain_id
from "query_4340428(blockchain='{{blockchain}}', competitor_end_time='{{competitor_end_time}}')"
