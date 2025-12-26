-- Markout calculation for UniswapX trades. Compares the price of prices.usd table at the time of the trade to the executed amounts.

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
    sell_token,
    buy_token,
    sell_price as sellprice,
    buy_price as buyprice,
    amount_usd,
    ((token_bought_amount/token_sold_amount) / (sell_price / buy_price)) - 1.0000 as markout
from "query_6033946(start_date='{{start_date}}', end_date='{{end_date}}')" as uniswapx
where 
    blockchain = '{{blockchain}}'
    and block_time >= timestamp '{{start_date}}'
    and block_time < timestamp '{{end_date}}'
    and token_bought_amount * buy_price between {{min_usd_amount}} and {{max_usd_amount}}
    and token_sold_amount * sell_price between {{min_usd_amount}} and {{max_usd_amount}}
