-- this query returns a daily list of buy and sell tokens which have a stable daily xrate between them in the previous 7d
-- stability criteria = relative volatility < 0.7% (stddev_price / avg_price) and price range < 2.5%  (max_price-min_price)/avg_price
-- these criteria are based on empiric evidence, but it's been observed that for some dates, pairs like stETH-WETH will show similar stats to USDT-WBTC, 
-- so at the end we force ETH to ETH variations to be included here
-- even though we'd like to categorise them respectively as stable and variable. 
with
relevant_tokens as (
    select 
        block_date as ref_date
        , '{{blockchain}}' as blockchain
        -- the following logic makes fetching the native token price possible
        , if(
            sell_token_address!=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
            , sell_token_address
            , (select token_address from dune.blockchains where name = '{{blockchain}}')
        ) as sell_token_address 
        , if(
            buy_token_address!=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
            , buy_token_address
            , (select token_address from dune.blockchains where name = '{{blockchain}}')
        ) as buy_token_address
        , sell_token
        , buy_token
        , token_pair
        , sum(usd_value) as daily_volume
    from cow_protocol_{{blockchain}}.trades
    where block_date between timestamp '{{start_date}}' and timestamp '{{end_date}}'
    group by 1,2,3,4,5,6,7
    having sum(usd_value) > 100 -- performance improvement w/ minimal impact
)
, daily_relevant_prices as (
    select 
        timestamp as day
        , contract_address
        , price
    from prices.day as p 
    join (
        select distinct buy_token_address  as token from relevant_tokens
        union distinct
        select distinct sell_token_address as token from relevant_tokens
    ) as t
        on p.contract_address = t.token
    where 
        timestamp between date_add('day', -6, timestamp '{{start_date}}') and timestamp '{{end_date}}'
        and blockchain = '{{blockchain}}'
)
, history as (
    select
        t.*,
        pb.day as price_date,
        pb.price as buy_token_price,
        ps.price as sell_token_price,
        ps.price / pb.price as xrate
    from relevant_tokens as t
    join daily_relevant_prices as pb
        on pb.contract_address = t.buy_token_address
        and pb.day between date_add('day', -6, t.ref_date) and t.ref_date
    join daily_relevant_prices as ps
        on ps.contract_address = t.sell_token_address
        and ps.day = pb.day -- avoid cartesian explosion
)
, final_stats as (
    select 
        ref_date
        , blockchain
        -- following logis is to undo what was done before and keep it joinable with the cp trades tables
        , if(
            sell_token_address!=(select token_address from dune.blockchains where name = '{{blockchain}}')
            ,sell_token_address
            ,0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        ) as sell_token_address        
        , if(
            buy_token_address!=(select token_address from dune.blockchains where name = '{{blockchain}}')
            ,buy_token_address
            ,0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        ) as buy_token_address
        , sell_token
        , buy_token
        , token_pair
        , daily_volume
        , stddev(xrate) / avg(xrate) as relative_volatility
        , (max(xrate)-min(xrate)) / avg(xrate) as xrate_range
        , avg(xrate)    as avg_xrate_l7d
        , stddev(xrate) as stddev_xrate_l7d
        , max(xrate)    as max_xrate_l7d
        , min(xrate)    as min_xrate_l7d
        , count(if(xrate is not null,1,null)) as days_of_data
    from history
    group by 1,2,3,4,5,6,7,8
)
select*
from final_stats
where (
    relative_volatility < 0.007   -- relative volatility < 0.7%
    and xrate_range < 0.025    -- xrate range within 2.5%
    and days_of_data > 4    -- at least 5 days of xrate data
) 
or token_pair like '%ETH%ETH'
