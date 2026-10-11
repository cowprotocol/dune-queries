with block_range as (
    select
        min("number") as start_block,
        max("number") as end_block
    from {{blockchain}}.blocks
    where time >= cast('{{start_time}}' as timestamp) and time < cast('{{end_time}}' as timestamp)
),

wrapped_native_token as (
    select
        case '{{blockchain}}'
            when 'ethereum' then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
            when 'gnosis' then 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d -- WXDAI
            when 'arbitrum' then 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- WETH
            when 'base' then 0x4200000000000000000000000000000000000006 -- WETH
            when 'avalanche_c' then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- WAVAX
            when 'polygon' then 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 -- WPOL
            when 'bnb' then 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c -- WBNB
            when 'linea' then 0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f -- WETH
            when 'plasma' then 0x6100e367285b01f48d07953803a2d8dca5d19873 -- WXPL
            when 'ink' then 0x4200000000000000000000000000000000000006 -- WETH
        end as native_token_address
),

all_trades as (
    select
        t.block_time,
        t.order_uid,
        t.tx_hash,
        t.sell_token_address,
        t.buy_token_address,
        case
            when t.order_type = 'SELL' then t.buy_token_address
            else t.sell_token_address
        end as problematic_token_address,
        t.usd_value,
        rd.auction_id,
        rd.environment,
        rd.protocol_fee,
        rd.protocol_fee * rd.protocol_fee_native_price * p.price / pow(10,18) as protocol_fee_collected
    from cow_protocol_{{blockchain}}.trades as t
    inner join "query_4364122(blockchain='{{blockchain}}')" as rd on
        t.order_uid = rd.order_uid
        and t.tx_hash = rd.tx_hash
        and t.block_number >= (select start_block from block_range)
        and t.block_number <= (select end_block from block_range)
    inner join prices.usd as p on
        date_trunc('minute', t.block_time) = p.minute
        and p.blockchain = '{{blockchain}}'
        and p.contract_address = (select native_token_address from wrapped_native_token)
    
),

filtered_trades as (
    select *
    from all_trades
    where protocol_fee_collected * 1.000 / usd_value > 0.02
)

select *
from filtered_trades
