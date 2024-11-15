-- Finds all the curve pools with 2 tokens and their TVLs
-- Input: blockchain

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
        block_time,
        fee,
        sum(transfer0) over (partition by contract_address order by block_time, evt_index) as reserve0,
        sum(transfer1) over (partition by contract_address order by block_time, evt_index) as reserve1,
        row_number() over (partition by tx_hash, contract_address order by evt_index desc) as row_num,
        row_number() over (partition by contract_address order by block_time desc) as latest
    from transfers
)

-- finds the TVL of the pools
select
    r.contract_address,
    token0,
    token1,
    block_time,
    tx_hash,
    reserve0,
    reserve1,
    fee,
    latest,
    (reserve0 * p0.price / pow(10, p0.decimals)) + (reserve1 * p1.price / pow(10, p1.decimals)) as tvl
from reserves as r
inner join prices.usd as p0
    on
        date_trunc('minute', block_time) = p0.minute
        and token0 = p0.contract_address
inner join prices.usd as p1
    on
        date_trunc('minute', block_time) = p1.minute
        and token1 = p1.contract_address
where row_num = 1
