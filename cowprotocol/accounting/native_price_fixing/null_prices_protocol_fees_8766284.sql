with block_range as (
    select
        min("number") as start_block,
        max("number") as end_block
    from {{blockchain}}.blocks
    where time >= cast('{{start_time}}' as timestamp) and time < cast('{{end_time}}' as timestamp)
),

all_trades as (
    select
        t.block_time,
        t.order_uid,
        t.tx_hash,
        t.sell_token_address,
        t.buy_token_address,
        t.usd_value,
        rd.auction_id,
        rd.environment,
        rd.protocol_fee,
        rd.protocol_fee * rd.protocol_fee_native_price * {{price_of_native_token_in_dollars}} / pow(10,18) as protocol_fee_collected
    from cow_protocol_{{blockchain}}.trades as t inner join "query_4364122(blockchain='{{blockchain}}')" as rd
        on t.order_uid = rd.order_uid and t.tx_hash = rd.tx_hash
        and t.block_number >= (select start_block from block_range) and t.block_number <= (select end_block from block_range)
),

filtered_trades as (
    select *
    from all_trades
    where usd_value is null
)

select *
from filtered_trades
