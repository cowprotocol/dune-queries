with
static as (
    select 
        split('{{blockchain}}', ',') as blockchains
        , '{{date_granularity}}' as date_granularity
        , (select start_date['{{period}}'] from "query_5633784") as start_date
)
, fusion_txs as ( -- all fusion txs are included in '1inch' in "dex_aggregator.trades", so we'll just do a join to differentiate
    select distinct
        tx_hash
        , blockchain
        , '1inch fusion' as project        
    from oneinch.swaps, static
    where
        flags['fusion'] 
        and block_time >= static.start_date
        and if(array_position(static.blockchains, '-=All=-') > 0, true, array_position(static.blockchains, blockchain) > 0) 
)
, uniswap_x_txs as ( -- some uniswap X txs are present in "dex_aggregator.trades", we'll first filter them out to avoid double counting
    select *
    from "query_5602624(period='{{period}}')", static
    where if(array_position(static.blockchains, '-=All=-') > 0, true, array_position(static.blockchains, blockchain) > 0) 
)
, dex_agg_trades_prep as (
    select
        t.block_time,
        t.blockchain,
        t.tx_hash,
        t.evt_index,
        t.trace_address,
        lower(coalesce(f.project, t.project)) as project, 
        t.amount_usd,
        t.token_sold_symbol,
        t.token_bought_symbol,
        t.token_sold_amount,
        t.token_bought_amount,
        t.taker as trader
    from dex_aggregator.trades as t 
    cross join static
    left join fusion_txs as f
        on t.tx_hash = f.tx_hash
    left join uniswap_x_txs as u
        on t.tx_hash = u.tx_hash
        and t.blockchain = u.blockchain
    where 
        t.block_time >= static.start_date
        and if(array_position(static.blockchains, '-=All=-') > 0, true, array_position(static.blockchains, t.blockchain) > 0) 
        and u.tx_hash is null 
)
-- for each tx, only keep project with shallowest trace_address
, dex_agg_trades_prep_2 as (
    select 
        block_time
        , blockchain
        , tx_hash
        , project
        , sum(amount_usd) as volume
        , count(distinct trader) as traders
        , count(1) as trades
        , min(trace_address) as proj_trace_address
    from dex_agg_trades_prep
    group by 1 ,2 ,3 ,4
)
, dex_agg_trades_prep_3 as (
    select *
    from (
        select 
            *
            , min(proj_trace_address) over (partition by tx_hash) as tx_min_trace_address
        from dex_agg_trades_prep_2
    )
    where proj_trace_address = tx_min_trace_address
)
, dex_agg_trades as (
    select 
        date_trunc('{{date_granularity}}', block_time) as date
        --, blockchain
        , project
        , sum(volume) as volume
        , sum(trades) as trades
        , sum(traders) as traders
    from dex_agg_trades_prep_3
    group by 1,2
)
, uniswapx as ( -- add uniswap X on top since it's always 1st layer
    select
        date_trunc(static.date_granularity, block_time) as date
        --, blockchain
        , 'uniswap x' as project
        , sum(amount_usd) as volume
        , count(1) as trades
        , count(distinct trader) as traders        
    from uniswap_x_txs, static
    group by 1,2
)
select * from dex_agg_trades
union all 
select * from uniswapx
order by date, project
