--- Markout calculation for non-intent DEX aggregator trades. Compares the price of dex.prices table at the time of the trade to the executed amounts and factors in gas costs.
--- Returns a per transaction hash buy and sell USD value, gas cost in USD and the resulting markout.

-- Parameters:
--  {{project}} - The aggregator to look at
--  {{start_date}} - Start date for when trades should be counted
--  {{end_date}} - End date for when trades should be counted
--  {{blockchain}} - The chain on which trades should be counted
--  {{native_token}} - The native token of the blockchain (e.g. ETH for Ethereum)
--  {{min_usd_amount}} - Minimum USD amount of the trade to be considered
--  {{max_usd_amount}} - Maximum USD amount of the trade to be considered

with markout_per_trade as (
    select 
        tx_hash,
        token_bought_amount,
        token_sold_amount,
        token_bought_address,
        token_sold_address,
        sell.price as sellprice,
        buy.price as buyprice,
        amount_usd,
        token_bought_amount * buy.price as buy_usd_value,
        token_sold_amount * sell.price as sell_usd_value
    from dex_aggregator.trades as t
    join prices.usd as sell
        on sell.contract_address = token_sold_address
        and sell.minute = date_trunc('minute', block_time)
        and sell.price > 0
        and sell.blockchain = t.blockchain
    join prices.usd as buy
        on buy.contract_address = token_bought_address
        and buy.minute = date_trunc('minute', block_time)
        and buy.price > 0
        and buy.blockchain = t.blockchain
    where
        t.blockchain = '{{blockchain}}'
        and project = '{{project}}'
        and t.block_time >= timestamp '{{start_date}}'
        and t.block_time < timestamp '{{end_date}}'
        and amount_usd >= {{min_usd_amount}} 
        and amount_usd <= {{max_usd_amount}}
),

markout_by_tx as (
    select 
        tx_hash, 
        sum(buy_usd_value) as buy_usd_value,
        sum(sell_usd_value) as sell_usd_value
    from markout_per_trade
    group by 1
)

select 
    tx_hash,
    buy_usd_value,
    sell_usd_value,
    (gas_used * gas_price * eth.price) / 1e18 as gas_usd,
    (buy_usd_value/(sell_usd_value + (gas_used * gas_price * eth.price) / 1e18 )) - 1.0000 as markout
from markout_by_tx
join {{blockchain}}.transactions
    on tx_hash = hash
join prices.usd as eth
    on eth.contract_address = {{native_token}}
    and eth.minute = date_trunc('minute', block_time)
    and eth.blockchain = '{{blockchain}}'
    -- filter out extreme native prices
    and buy_usd_value >= {{min_usd_amount}}
    and buy_usd_value <= {{max_usd_amount}}
    and sell_usd_value >= {{min_usd_amount}}
    and sell_usd_value <= {{max_usd_amount}}
order by markout asc
