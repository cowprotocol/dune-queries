-- This query computes fees
--
-- It currently uses the raw_order_rewards table which is only available on ethereum. On other chains it returns an empty table.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- The columns of the result are
-- - block_time: time of settlement transaction
-- - tx_hash: settlement transaction hash
-- - token_address: address of token with a balance change. contract address for erc20 tokens,
--   0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee for native token
-- - amount: value of fee in atoms of the token
-- - transfer_type: 'protocol_fee' for the total protocol fee (including partner fee), 'network_fee' for network fees

with fee_data as (
    select
        block_time,
        t.tx_hash,
        t.order_uid,
        atoms_sold,
        atoms_bought,
        sell_token_address,
        cast(cast(data.protocol_fee as varchar) as int256) as protocol_fee, -- noqa: RF01
        cast(cast(data.surplus_fee as varchar) as int256) as surplus_fee, -- noqa: RF01
        from_hex(data.protocol_fee_token) as protocol_fee_token_address -- noqa: RF01
    from cow_protocol_{{blockchain}}.trades as t
    inner join cowswap.raw_order_rewards as ror -- this table only exists for ethereum trades
        on t.order_uid = from_hex(ror.order_uid) and t.tx_hash = from_hex(ror.tx_hash)
    where block_time >= cast('{{start_time}}' as timestamp) and block_time < cast('{{end_time}}' as timestamp)
),

protocol_fee_balance_changes as (
    select
        block_time,
        tx_hash,
        protocol_fee_token_address as token_address,
        protocol_fee as amount,
        'protocol_fee' as transfer_type
    from fee_data
),

network_fee_balance_changes as (
    select
        block_time,
        tx_hash,
        sell_token_address as token_address,
        'network_fee' as transfer_type,
        case
            when sell_token_address = protocol_fee_token_address then surplus_fee - protocol_fee
            else surplus_fee - cast(1.0 * protocol_fee * (atoms_sold - surplus_fee) / atoms_bought as int256)
        end as amount
    from fee_data
)

select * from protocol_fee_balance_changes
union all
select * from network_fee_balance_changes
