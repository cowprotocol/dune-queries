-- Computes the volume weighted price ratio between the specified aggregator and dune prices at the time of trade per token pair.
-- A ratio >1 means the aggregator is providing better prices
--
-- Parameters:
--  {{project}} - The aggregator to look at
--  {{start}} - Start date for when trades should be counted
--  {{blockchain}} - The chain on which trades should be counted

with trades as (
    select
        block_time,
        amount_usd,
        token_bought_symbol,
        token_sold_symbol,
        (token_bought_amount / token_sold_amount) as project_price,
        p1.price / p0.price as dune_price
    from dex_aggregator.trades as p
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
        and token_sold_amount > 0
        and p1.price > 0
        and p.blockchain = '{{blockchain}}'
        and project = '{{project}}'
)

select
    token_bought_symbol as buy_token,
    token_sold_symbol as sell_token,
    -- geometric volume-weighted price ratio
    exp(sum(amount_usd * ln(project_price / dune_price)) / sum(amount_usd)) as dune_price_ratio,
    sum(amount_usd) as volume
from trades
group by 1, 2
