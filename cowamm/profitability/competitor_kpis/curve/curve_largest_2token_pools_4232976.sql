-- Finds all the curve pools with 2 tokens and their TVLs
-- Input: blockchain

with 
-- filters pools with 2 tokens
pools as(
select pool_address as contract_address,
coin0 as token0,
coin1 as token1,
mid_fee*power(10,-10) as fee
from curvefi_{{blockchain}}.view_pools
where coin2 = 0x0000000000000000000000000000000000000000),

-- finds all transfers in and out of the pools to rebuild the reserves
transfers as(
select p.contract_address, token0, token1, -value as transfer0, 0 as transfer1, evt_index, evt_tx_hash as tx_hash, evt_block_time as time, fee
    from pools p
    join erc20_{{blockchain}}.evt_transfer t0
    on p.contract_address = t0."from"
    and p.token0 = t0.contract_address
union
select p.contract_address, token0, token1, value as transfer0, 0 as transfer1, evt_index, evt_tx_hash as tx_hash, evt_block_time as time, fee
    from pools p
    join erc20_{{blockchain}}.evt_transfer t0
    on p.contract_address = t0.to
    and p.token0 = t0.contract_address
union
select p.contract_address, token0, token1, 0 as transfer0, -value as transfer1, evt_index, evt_tx_hash as tx_hash, evt_block_time as time, fee
    from pools p
    join erc20_{{blockchain}}.evt_transfer t1
    on p.contract_address = t1."from"
    and p.token1 = t1.contract_address
union
select p.contract_address, token0, token1, 0 as transfer0, value as transfer1, evt_index, evt_tx_hash as tx_hash, evt_block_time as time, fee
    from pools p
    join erc20_{{blockchain}}.evt_transfer t1
    on p.contract_address = t1.to
    and p.token1 = t1.contract_address),

-- rebuilds the reserves from the transfers
-- ETH transfers are not considered  
reserves as(
select contract_address, token0, token1, tx_hash, time,
sum(transfer0) over (partition by contract_address order by time, evt_index) as reserve0,
sum(transfer1) over (partition by contract_address order by time, evt_index) as reserve1,
fee,
ROW_NUMBER() OVER (PARTITION BY tx_hash,contract_address ORDER BY evt_index DESC) AS row_num,
ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY time DESC) AS latest
from transfers)

-- finds the TVL of the pools
select r.contract_address, token0, token1, time, tx_hash,
reserve0, reserve1,
(reserve0 * p0.price / pow(10, p0.decimals)) + (reserve1 * p1.price / pow(10, p1.decimals)) as tvl,
fee, latest
from reserves r
inner join prices.usd as p0
        on
            date_trunc('minute', time) = p0.minute
            and token0 = p0.contract_address
    inner join prices.usd as p1
        on
            date_trunc('minute', time) = p1.minute
            and token1 = p1.contract_address 
where row_num = 1

