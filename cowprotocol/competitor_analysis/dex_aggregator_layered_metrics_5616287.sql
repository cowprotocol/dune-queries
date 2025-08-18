-- Calculates metrics based on the project of the top layer of a transaction, regardless of whether it routes through other dex aggregators
with
static as (
    select 
        split('{{blockchain}}', ',') as blockchains
        , '{{date_granularity}}' as date_granularity
        , (select start_date['{{period}}'] from "query_5633784") as start_date
)
-- All fusion txs are included in '1inch' in "dex_aggregator.trades", so we'll just do a join+coalesce to differentiate
, fusion_txs as ( 
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
-- Some uniswap X txs are present in "dex_aggregator.trades", we'll first filter them out to avoid double counting
, uniswap_x_txs as ( 
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
-- Aggregate raw trades data by transaction and project
-- Also get the earliest trace position for that project within the transaction
, dex_agg_trades_prep_2 as (
    select 
        block_time
        , blockchain
        , tx_hash
        , project
        , sum(amount_usd) as volume
        , count(distinct trader) as traders
        , count(*) as trades
        , min(trace_address) as proj_1st_trace_address
        , min(evt_index) as proj_1st_evt_index
    from dex_agg_trades_prep
    group by 1 ,2 ,3 ,4
)
-- From all projects in a tx, keep only those with the shallowest trace_address (1st in execution)
, dex_agg_trades_prep_3 as (
    select *
    from (
        select 
            *
            , min(proj_1st_trace_address) over (partition by tx_hash) as tx_1st_trace_address
        from dex_agg_trades_prep_2
    )
    where 
        proj_1st_trace_address = tx_1st_trace_address
)
-- If multiple projects share the same trace_address, break ties using evt_index
, dex_agg_trades_prep_4 as (
    select *
    from (
        select 
            *
            , min(proj_1st_evt_index) over (partition by tx_hash) as tx_1st_evt_index
        from dex_agg_trades_prep_3
    )
    where 
        proj_1st_evt_index = tx_1st_evt_index
)
, dex_agg_trades as (
    select 
        date_trunc('{{date_granularity}}', block_time) as date
        , project
        , sum(volume) as volume
        , sum(trades) as trades
        , sum(traders) as traders
    from dex_agg_trades_prep_4
    group by 1,2
)
, uniswapx as ( -- add uniswap X on top since it's always 1st layer
    select
        date_trunc(static.date_granularity, block_time) as date
        , 'uniswap x' as project
        , sum(amount_usd) as volume
        , count(*) as trades
        , count(distinct trader) as traders        
    from uniswap_x_txs, static
    group by 1,2
)
select * from dex_agg_trades
union all 
select * from uniswapx
order by date, project
