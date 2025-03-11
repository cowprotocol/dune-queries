-- Calculates transactions USD volume by sender, where only top-level trade is kept.
-- Takes into account both dex and dex-aggregators.
--
-- Parameters:
--  {{start_time}} - the trade timestamp for which the analysis should start (inclusive)
--  {{end_time}} - the trade timestamp for which the analysis should end (inclusive)

with dex_aggregator_trades as (
    select
        da_trades.tx_from,
        da_trades.tx_hash,
        da_trades.blockchain,
        da_trades.project,
        sum(da_trades.amount_usd) as amount_usd,
        any_value(da_trades.block_time) as block_time,
        case
            -- Priorities are chosen in a way that this measures which interface the volume was traded from.
            when da_trades.project = 'cow_protocol' then 1 -- GP aggregates the aggregators
            when da_trades.project in ('0x API', '1inch', '1inch Limit Order Protocol') then 2 -- Matcha uses 0x API as a sub aggregator
            else 3
        end as priority,
        'dex-aggregator' as product_type
    from
        dex_aggregator.trades as da_trades
    -- Avoid double counting 1inch fusion
    left join query_3860172 as _1inch_fusion
        on
            da_trades.tx_hash = _1inch_fusion.tx_hash
            and
            da_trades.blockchain = _1inch_fusion.blockchain
    -- Avoid double counting Paraswap delta on ethereum
    left join query_4048962 as paraswap_delta
        on
            da_trades.tx_hash = paraswap_delta.evt_tx_hash
            -- and TODO: This might be needed
            -- da_trades.blockchain = 'ethereum'
    where
        da_trades.block_time between timestamp '{{start_time}}' and timestamp '{{end_time}}'
        and
        -- Avoid faulty transactions
        not da_trades.tx_hash in (select tx_hash from query_2617370)
        and
        _1inch_fusion.tx_hash is null
        and
        paraswap_delta.evt_tx_hash is null
    group by
        1, 2, 3, 4
),

dex_trades as (
    select
        tx_from,
        tx_hash,
        blockchain,
        project,
        sum(amount_usd) as amount_usd,
        any_value(block_time) as block_time,
        4 as priority, -- ensure it goes after all dex-agg priorities
        'dex' as product_type
    from
        dex.trades
    where
        block_time between timestamp '{{start_time}}' and timestamp '{{end_time}}'
    group by
        1, 2, 3, 4
),

unified as (
    select *
    from
        dex_aggregator_trades
    union all
    select *
    from
        dex_trades
),

keep_first_layer_only as (
    select
        tx_from,
        tx_hash,
        blockchain,
        project,
        amount_usd,
        block_time,
        product_type
    from (
        select
            *,
            row_number() over (partition by tx_from, blockchain, tx_hash order by priority asc) as rn
        from
            unified
    )
    where
        rn = 1
)

select *
from
    keep_first_layer_only
