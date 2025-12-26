-- Markout calculation for CoW Protocol trades. Compares the price of prices.usd table at the time of the trade to the executed amounts.

-- Parameters:
--  {{blockchain}} - The chain on which trades should be counted
--  {{start_date}} - Start date for when trades should be counted
--  {{end_date}} - End date for when trades should be counted
--  {{min_usd_amount}} - Minimum USD amount of the trade to be considered
--  {{max_usd_amount}} - Maximum USD amount of the trade to be considered

select 
    block_time,
    tx_hash,
    token_bought_amount,
    token_sold_amount,
    token_bought_address,
    token_sold_address,
    token_bought_symbol,
    token_sold_symbol,
    sell.price as sellprice,
    buy.price as buyprice,
    amount_usd,
    ((token_bought_amount/token_sold_amount) / (sell.price / buy.price)) - 1.0000 as markout
from cow_protocol.trades as t
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
    and token_pair is not null
    and block_date >= timestamp '{{start_date}}'
    and block_date < timestamp '{{end_date}}'
    and token_bought_amount * buy.price between {{min_usd_amount}} and {{max_usd_amount}}
    and token_sold_amount * sell.price between {{min_usd_amount}} and {{max_usd_amount}}
