-- base query with data per order
select --noqa: ST06
    cast(block_number as int256) as block_number,
    from_hex(order_uid) as order_uid,
    from_hex(solver) as solver,
    from_hex(tx_hash) as tx_hash,
    -- Unpacking the data
    cast(data.surplus_fee as int256) as surplus_fee, --noqa: RF01, RF03
    cast(data.amount as double) as reward, --noqa: RF01
    from_hex(data.quote_solver) as quote_solver, --noqa: RF01
    cast(data.protocol_fee as int256) as protocol_fee, --noqa: RF01
    from_hex(data.protocol_fee_token) as protocol_fee_token, --noqa: RF01
    cast(data.protocol_fee_native_price as double) as protocol_fee_native_price, --noqa: RF01
    data.protocol_fee_kind, --noqa: RF01
    from_hex(data.partner_fee_recipient) as partner_fee_recipient --noqa: RF01
from cowswap.raw_order_rewards
