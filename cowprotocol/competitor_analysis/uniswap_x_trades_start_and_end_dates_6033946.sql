--noqa: disable=LT09
with 
params as (
    select 
        timestamp'{{start_date}}' as start_date
        , timestamp'{{end_date}}' as end_date
) 
-- currently uniswap x has different types of reactors depending on the blockchain
, uniswap_x_fills as (
    select chain as blockchain, evt_block_time, evt_tx_hash, swapper
    from uniswap_multichain.priorityorderreactor_evt_fill, params -- base and unichain
    where 
        evt_block_time >= params.start_date
        and evt_block_time < params.end_date
    union all
    select chain as blockchain, evt_block_time, evt_tx_hash, swapper
    from uniswap_multichain.v2dutchorderreactor_evt_fill, params -- mainnet and arbitrum
    where 
        evt_block_time >= params.start_date
        and evt_block_time < params.end_date 
    union all
    select 'ethereum' as blockchain, evt_block_time, evt_tx_hash, swapper
    from uniswap_ethereum.exclusivedutchorderreactor_evt_fill, params
    where 
        evt_block_time >= params.start_date
        and evt_block_time < params.end_date
    union all
    select 'arbitrum' as blockchain, evt_block_time, evt_tx_hash, swapper
    from uniswap_arbitrum.v3dutchorderreactor_evt_fill, params
    where 
        evt_block_time >= params.start_date
        and evt_block_time < params.end_date
)
--------------------------------------------------------------------------------------------------------------------------------------------
-- fills from tables above have no data on which tokens are being traded and their amounts, so we must add them manually
, transfers as (
    --erc20 tokens 
    select 'ethereum' as blockchain, evt_tx_hash as tx_hash, "from", "to", value, contract_address
    from erc20_ethereum.evt_transfer, params  --erc20 tokens
    where evt_block_time >= params.start_date
        and evt_block_time < params.end_date
        and value > 0

    union all 
    select 'arbitrum' as blockchain, evt_tx_hash as tx_hash, "from", "to", value, contract_address
    from erc20_arbitrum.evt_transfer, params  --erc20 tokens
    where evt_block_time >= params.start_date
        and evt_block_time < params.end_date
        and value > 0        

    union all 
    select 'base' as blockchain, evt_tx_hash as tx_hash, "from", "to", value, contract_address
    from erc20_base.evt_transfer, params  --erc20 tokens
    where evt_block_time >= params.start_date
        and evt_block_time < params.end_date
        and value > 0 

    union all 
    select 'unichain' as blockchain, evt_tx_hash as tx_hash, "from", "to", value, contract_address
    from erc20_unichain.evt_transfer, params  --erc20 tokens
    where evt_block_time >= params.start_date
        and evt_block_time < params.end_date
        and value > 0
    
    --native tokens
    union all
    select 'ethereum' as blockchain, tx_hash, "from", "to", value, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as contract_address -- make it WETH to join with prices.usd
    from ethereum.traces, params  -- native token
    where 
        success
        and block_time >= params.start_date
        and block_time < params.end_date
        and value > 0
    
    union all     
    select 'arbitrum' as blockchain, tx_hash, "from", "to", value, 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as contract_address -- make it WETH to join with prices.usd
    from arbitrum.traces, params  -- native token
    where 
        success
        and block_time >= params.start_date
        and block_time < params.end_date
        and value > 0

    union all 
    select 'base' as blockchain, tx_hash, "from", "to", value, 0x4200000000000000000000000000000000000006 as contract_address -- make it WETH to join with prices.usd
    from base.traces, params  -- native token
    where 
        success
        and block_time >= params.start_date
        and block_time < params.end_date
        and value > 0

    union all 
    select 'unichain' as blockchain, tx_hash, "from", "to", value, 0x8f187aa05619a017077f5308904739877ce9ea21 as contract_address -- UNI token
    from unichain.traces, params  -- native token
    where 
        success
        and block_time >= params.start_date
        and block_time < params.end_date
        and value > 0
)
--------------------------------------------------------------------------------------------------------------------------------------------
-- prep prices in order to get amounts in USD
, prices as (
    select blockchain, minute, contract_address, decimals, price, symbol
    from prices.usd , params 
    where 
        minute >= params.start_date
        and minute < params.end_date
        and blockchain in ('ethereum', 'arbitrum', 'base', 'unichain')
)
--------------------------------------------------------------------------------------------------------------------------------------------
select 
    f.blockchain
    , f.evt_block_time as block_time
    , f.evt_tx_hash as tx_hash
    , f.swapper as trader
    , tfr_sell.contract_address as token_sold_address
    , tfr_buy.contract_address as token_bought_address
    , pr_sell.symbol as sell_token
    , pr_buy.symbol as buy_token
    , tfr_sell.value / pow(10, pr_sell.decimals) as token_sold_amount
    , tfr_buy.value / pow(10, pr_buy.decimals) as token_bought_amount
    , pr_sell.price as sell_price
    , pr_buy.price as buy_price
    , greatest(
        coalesce(tfr_sell.value / pow(10, pr_sell.decimals) * pr_sell.price, 0)
        , coalesce(tfr_buy.value / pow(10, pr_buy.decimals) * pr_buy.price, 0)
    ) as amount_usd
        
from uniswap_x_fills as f 

left join transfers as tfr_sell 
    on f.evt_tx_hash = tfr_sell.tx_hash 
    and f.swapper = tfr_sell."from"        
    and f.blockchain = tfr_sell.blockchain   
    
left join prices as pr_sell 
    on tfr_sell.contract_address = pr_sell.contract_address 
    and date_trunc('minute', f.evt_block_time) = pr_sell.minute
    and f.blockchain = pr_sell.blockchain

left join transfers as tfr_buy 
    on f.evt_tx_hash = tfr_buy.tx_hash 
    and f.swapper = tfr_buy."to"        
    and f.blockchain = tfr_buy.blockchain   
    
left join prices as pr_buy 
    on tfr_buy.contract_address = pr_buy.contract_address 
    and date_trunc('minute', f.evt_block_time) = pr_buy.minute
    and f.blockchain = pr_buy.blockchain
