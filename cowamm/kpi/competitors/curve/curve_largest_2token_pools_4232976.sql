-- Finds all the curve pools with 2 tokens and their TVLs at every moment in time
-- For every pool that is currently one of the {{number_of_pools}} largest by TVL:
-- It returns their TVL at every moment in time
-- Parameters:
--  {{blockchain}}: The blockchain to query
--  {{number_of_pools}}: The number of largest pools to return
-- {{start_time}}: The start time of the analysis. date '{{start_time}}' <= evt_block_time < date '{{start_time}}' + 1 day
--      By default, we look at the past full day

with
-- filters pools with 2 tokens
pools as (
    select
        pool_address as contract_address,
        coin0 as token0,
        coin1 as token1,
        mid_fee * power(10, -10) as fee
    from curvefi_{{blockchain}}.view_pools
    -- Curve pools can have more than 2 token, if coin2 = 0x00 it means there are 2 tokens at most
    where coin2 = 0x0000000000000000000000000000000000000000
),

-- finds all transfers in and out of the pools to rebuild the reserves
transfers as (
    select
        p.contract_address,
        token0,
        token1,
        evt_block_time as block_time,
        evt_index,
        evt_tx_hash as tx_hash,
        fee,
        case
            when t.contract_address = token0 and "from" = p.contract_address then -value
            when t.contract_address = token0 and to = p.contract_address then value else 0
        end as transfer0,
        case
            when t.contract_address = token1 and "from" = p.contract_address then -value
            when t.contract_address = token1 and to = p.contract_address then value else 0
        end as transfer1
    from erc20_{{blockchain}}.evt_transfer as t
    inner join pools as p
        on
            (
                t."from" = p.contract_address
                or t.to = p.contract_address
            )
            and (token0 = t.contract_address or token1 = t.contract_address)
),

-- rebuilds the reserves from the transfers
-- ETH transfers are not considered 
reserves as (
    select
        contract_address,
        token0,
        token1,
        tx_hash,
        block_time as evt_block_time,
        fee,
        sum(transfer0) over (partition by contract_address order by block_time, evt_index) as reserve0,
        sum(transfer1) over (partition by contract_address order by block_time, evt_index) as reserve1,
        row_number() over (partition by tx_hash, contract_address order by evt_index desc) as latest_per_tx,
        row_number() over (partition by contract_address order by block_time desc, evt_index desc) as latest_per_pool
    from transfers
    where block_time <= least(date_add('day', 1, date('{{start_time}}')), date(now()))
),

-- finds the TVL of the pools
latest_tvl as (
    select
        r.contract_address,
        token0,
        token1,
        (reserve0 * p0.price / pow(10, p0.decimals)) + (reserve1 * p1.price / pow(10, p1.decimals)) as tvl
    from reserves as r
    --using the daily value to get a better representation of the TVL over the 24 hour period
    inner join prices.day as p0
        on
            p0.timestamp = least(date('{{start_time}}'), date_add('day', -1, date(now())))
            and token0 = p0.contract_address
    inner join prices.day as p1
        on
            p1.timestamp = least(date('{{start_time}}'), date_add('day', -1, date(now())))
            and token1 = p1.contract_address
    where latest_per_pool = 1
    order by tvl desc
    limit {{number_of_pools}}
)

select
    reserves.*,
    tvl
from reserves
inner join latest_tvl
    on reserves.contract_address = latest_tvl.contract_address
where latest_per_tx = 1
