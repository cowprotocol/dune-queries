-- Computes the volume weighted price ratio between the specified aggregator and dune prices at the time of trade per token pair.
-- A ratio >1 means the aggregator is providing better prices
--
-- Parameters:
--  {{project}} - The aggregator to look at
--  {{start}} - Start date for when trades should be counted
--  {{blockchain}} - The chain on which trades should be counted
--  {{top_n_pairs}} - Based on total DEX trading volume how many of the top token pairs to consider

with token_pairs as (
    select
        token_pair,
        sum(amount_usd) as volume
    from dex_aggregator.trades
    where block_time >= timestamp '{{start}}'
    group by 1
    order by 2 desc
    limit {{top_n_pairs}}
),

trades as (
    select
        block_time,
        amount_usd,
        token_bought_symbol,
        token_sold_symbol,
        (token_bought_amount / token_sold_amount) as project_price,
        p1.price / p0.price as dune_price
    from dex_aggregator.trades as p
    inner join token_pairs
        on p.token_pair = token_pairs.token_pair
    inner join prices.usd as p0
        on
            p0.contract_address = token_bought_address
            and p0.minute = date_trunc('minute', p.block_time)
    inner join prices.usd as p1
        on
            p1.contract_address = token_sold_address
            and p1.minute = date_trunc('minute', p.block_time)
    where
        block_time >= timestamp '{{start}}'
        and amount_usd > 100
        and token_bought_amount > 0
        and token_sold_amount > 0
        and p0.price > 0
        and p1.price > 0
        and p.blockchain = '{{blockchain}}'
        and project = '{{project}}'
)

select
    token_bought_symbol as buy_token,
    token_sold_symbol as sell_token,
    sum(amount_usd * project_price / dune_price) / sum(amount_usd) as dune_price_ratio,
    sum(amount_usd) as volume
from trades
group by 1, 2
