with block_range as (
    select *
    from "query_3333356(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

indexed_trades as (
    select *
    from cow_protocol_{{blockchain}}.trades
    where block_number >= (select start_block from block_range) and block_number <= (select end_block from block_range)
),

raw_trades as (
    select *
    from "query_4364122(blockchain='{{blockchain}}')"
    where block_number >= (select start_block from block_range) and block_number <= (select end_block from block_range)
),

indexed_minus_raw as (
    select
        tx_hash,
        order_uid,
        block_number
    from indexed_trades
    except
    select
        tx_hash,
        order_uid,
        block_number
    from raw_trades
),

raw_minus_indexed as (
    select
        tx_hash,
        order_uid,
        block_number
    from raw_trades
    except
    select
        tx_hash,
        order_uid,
        block_number
    from indexed_trades
),

results as (
    select
        *,
        true as dune
    from indexed_minus_raw
    union all
    select
        *,
        false as dune
    from raw_minus_indexed
)

select
    a.time,
    b.*
from {{blockchain}}.blocks as a inner join results as b on a.number = b.block_number
order by a.time
