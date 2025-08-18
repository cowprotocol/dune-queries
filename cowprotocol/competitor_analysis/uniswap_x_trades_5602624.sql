with
static as (
    select (select start_date['{{period}}'] from "query_5633784") as start_date
) 
-- we need different tables for different chains (not just a parameter change)
, prep_uniswapx_ethereum as (
    select * 
    from uniswap_ethereum.exclusivedutchorderreactor_evt_fill, static 
    where evt_block_time >= static.start_date
    union all        
    select * 
    from uniswap_ethereum.v2dutchorderreactor_evt_fill, static 
    where evt_block_time >= static.start_date
)
, prep_uniswapx_arbitrum as (
    select * 
    from uniswap_arbitrum.v2dutchorderreactor_evt_fill, static 
    where evt_block_time >= static.start_date
    union all        
    select * 
    from uniswap_arbitrum.v3dutchorderreactor_evt_fill, static 
    where evt_block_time >= static.start_date
)
--------------------------------------------------------------------------------------------------------------------------------------------
-- prep transfers in order to get amounts traded
, transfers_ethereum as (
    select
        evt_tx_hash as tx_hash,
        "from",
        "to",
        value,
        contract_address
    from erc20_ethereum.evt_transfer, static  --erc20 tokens
    where evt_block_time >= static.start_date  

    union all 
    select
        tx_hash,
        "from",
        "to",
        value,
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as contract_address -- make it WETH to join with prices.usd
    from ethereum.traces, static  -- native token
    where
        success
        and block_time >= static.start_date 
        and value > 0
)
, transfers_arbitrum as (
    select
        evt_tx_hash as tx_hash,
        "from",
        "to",
        value,
        contract_address
    from erc20_arbitrum.evt_transfer, static  --erc20 tokens
    where evt_block_time >= static.start_date  

    union all 
    select
        tx_hash,
        "from",
        "to",
        value,
        0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as contract_address -- make it WETH to join with prices.usd
    from arbitrum.traces, static  -- native token
    where
        success
        and block_time >= static.start_date 
        and value > 0
)
--------------------------------------------------------------------------------------------------------------------------------------------
-- prep prices in order to get amounts in USD
, prices_ethereum as (
    select
        minute,
        contract_address,
        decimals,
        price
    from prices.usd , static 
    where
        minute >= static.start_date
        and blockchain = 'ethereum'
)
, prices_arbitrum as (
    select
        minute,
        contract_address,
        decimals,
        price
    from prices.usd , static 
    where
        minute >= static.start_date
        and blockchain = 'arbitrum'
)
--------------------------------------------------------------------------------------------------------------------------------------------
, uniswapx_ethereum as (
    select 
        frm.evt_block_time as block_time
        , frm.evt_tx_hash as tx_hash
        , frm.orderhash as order_hash
        , frm.swapper as trader
        , greatest(
            coalesce(tfr_sell.value / pow(10, pr_sell.decimals) * pr_sell.price, 0)
            , coalesce(tfr_buy.value / pow(10, pr_buy.decimals) * pr_buy.price, 0)
        ) as amount_usd
            
    from prep_uniswapx_ethereum as frm
    cross join static     
    left join transfers_ethereum as tfr_sell 
        on frm.evt_tx_hash = tfr_sell.tx_hash 
        and frm.swapper = tfr_sell."from"        
        
    left join transfers_ethereum as tfr_buy 
        on frm.evt_tx_hash = tfr_buy.tx_hash 
        and frm.swapper = tfr_buy."to"        
        
    left join prices_ethereum as pr_sell 
        on tfr_sell.contract_address = pr_sell.contract_address 
        and date_trunc('minute', frm.evt_block_time) = pr_sell.minute
        
    left join prices_ethereum as pr_buy 
        on tfr_buy.contract_address = pr_buy.contract_address 
        and date_trunc('minute', frm.evt_block_time) = pr_buy.minute
)
, uniswapx_arbitrum as (
    select 
        frm.evt_block_time as block_time
        , frm.evt_tx_hash as tx_hash
        , frm.orderhash as order_hash
        , frm.swapper as trader
        , greatest(
            coalesce(tfr_sell.value / pow(10, pr_sell.decimals) * pr_sell.price, 0)
            , coalesce(tfr_buy.value / pow(10, pr_buy.decimals) * pr_buy.price, 0)
        ) as amount_usd
    from prep_uniswapx_arbitrum as frm
    cross join static 
    left join transfers_arbitrum as tfr_sell 
        on frm.evt_tx_hash = tfr_sell.tx_hash 
        and frm.swapper = tfr_sell."from"        
        
    left join transfers_arbitrum as tfr_buy 
        on frm.evt_tx_hash = tfr_buy.tx_hash 
        and frm.swapper = tfr_buy."to"        
        
    left join prices_arbitrum as pr_sell 
        on tfr_sell.contract_address = pr_sell.contract_address 
        and date_trunc('minute', frm.evt_block_time) = pr_sell.minute
        
    left join prices_arbitrum as pr_buy 
        on tfr_buy.contract_address = pr_buy.contract_address 
        and date_trunc('minute', frm.evt_block_time) = pr_buy.minute
)

select *, 'ethereum' as blockchain --noqa: LT09
from uniswapx_ethereum
union all 
select *, 'arbitrum' as blockchain --noqa: LT09
from uniswapx_arbitrum
