-- Markout calculation for non-intent DEX aggregator trades. 
-- Compares the price of prices.usd table at the time of the trade to the executed amounts and factors in gas costs.
-- Returns buy and sell USD value, gas cost in USD and the resulting markout, all per tx hash.

-- Parameters:
--  {{project}} - The aggregator to consider
--  {{start_date}} - Start date for when trades should be counted, inclusive
--  {{end_date}} - End date for when trades should be counted, exclusive
--  {{blockchain}} - The chain on which trades should be counted
--  {{min_usd_amount}} - Minimum USD amount of the trade to be considered
--  {{max_usd_amount}} - Maximum USD amount of the trade to be considered

with
markout_per_trade as (
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
        and if(project='0x API', '0x', project) = '{{project}}' -- this fixes a weird parameter bug 
        and t.block_time >= timestamp '{{start_date}}'
        and t.block_time < timestamp '{{end_date}}'
        and amount_usd between {{min_usd_amount}} and {{max_usd_amount}}
),
-- remove 1inch intent based txs
oneinch_intent_based as (
    select distinct tx_hash
    from oneinch.swaps
    where 
        mode in ('fusion', 'cross-chain')
        and block_time >= timestamp '{{start_date}}'
        and block_time < timestamp '{{end_date}}'
),
agg_by_tx as (
    select
        a.tx_hash, 
        sum(buy_usd_value) as buy_usd_value,
        sum(sell_usd_value) as sell_usd_value
    from markout_per_trade as a
    left join oneinch_intent_based as b
        on a.tx_hash = b.tx_hash
    where b.tx_hash is null
    group by 1
)
, native_prices as (
    select
        minute,
        price
    from prices.usd
    where 
        contract_address = (select price_address from tokens.native where chain = '{{blockchain}}')
        and blockchain = '{{blockchain}}'
        and minute >= timestamp '{{start_date}}'
        and minute < timestamp '{{end_date}}'
)
select 
    aggtx.tx_hash,
    aggtx.buy_usd_value,
    aggtx.sell_usd_value,
    (tx.gas_used * tx.gas_price * np.price) / 1e18 as gas_usd,
    (aggtx.buy_usd_value/(aggtx.sell_usd_value + (tx.gas_used * tx.gas_price * np.price) / 1e18 )) - 1.0000 as markout
from agg_by_tx as aggtx
join {{blockchain}}.transactions as tx
    on aggtx.tx_hash = tx.hash
join native_prices as np
    on np.minute = date_trunc('minute', tx.block_time)
where
    aggtx.buy_usd_value between {{min_usd_amount}} and {{max_usd_amount}}
    and aggtx.sell_usd_value between {{min_usd_amount}} and {{max_usd_amount}}
order by markout
