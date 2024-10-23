-- base query with data per order
select 
    cast(block_number as int256) as block_number,
    from_hex(order_uid) as order_uid,
    from_hex(solver) as solver,
    from_hex(tx_hash) as tx_hash,
    -- Unpacking the data
    cast(data.surplus_fee as int256) as surplus_fee,
    cast(data.amount as double) as reward,
    from_hex(data.quote_solver) as quote_solver,
    cast(data.protocol_fee as int256) as protocol_fee,
    from_hex(data.protocol_fee_token) as protocol_fee_token,
    cast(data.protocol_fee_native_price as double) as protocol_fee_native_price,
    data.protocol_fee_kind as protocol_fee_kind,
    from_hex(data.partner_fee_recipient) as partner_fee_recipient
from cowswap.raw_order_rewards