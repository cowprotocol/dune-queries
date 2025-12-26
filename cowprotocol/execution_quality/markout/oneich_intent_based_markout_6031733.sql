-- 1inch fusion markout calculation. Compares the price of prices.usd table at the time of the trade to the executed amounts

-- Parameters:
--  {{blockchain}} - The chain on which trades should be counted
--  {{start_date}} - Start date for when trades should be counted
--  {{end_date}} - End date for when trades should be counted
--  {{min_usd_amount}} - Minimum USD amount of the trade to be considered
--  {{max_usd_amount}} - Maximum USD amount of the trade to be considered

select
    tx_hash,
    cast(dst_executed_amount as double) /pow(10, cast(complement['dst_decimals'] as bigint)) as dst_exec_amount,
    cast(src_executed_amount as double) /pow(10, cast(complement['src_decimals'] as bigint)) as src_exec_amount,
    dst_token_address,
    src_token_address,
    sell.price as sellprice,
    buy.price as buyprice,
    amount_usd,
    (cast(dst_executed_amount as double) /pow(10, cast(complement['dst_decimals'] as bigint))) / (cast(src_executed_amount as double)/pow(10, cast(complement['src_decimals'] as bigint))) / (sell.price / buy.price) - 1.0000 as markout
from oneinch.swaps as t
join prices.usd as sell
    on sell.contract_address = src_token_address
    and sell.minute = date_trunc('minute', block_time)
    and sell.price > 0
    and sell.blockchain = '{{blockchain}}'
join prices.usd as buy
    on buy.contract_address = dst_token_address
    and buy.minute = date_trunc('minute', block_time)
    and buy.price > 0
    and buy.blockchain = '{{blockchain}}'
where
    t.blockchain = '{{blockchain}}'
    and block_time >= timestamp '{{start_date}}'
    and block_time < timestamp '{{end_date}}'
    and amount_usd between {{min_usd_amount}} and {{max_usd_amount}}
    and mode in ('fusion', 'cross_chain')
