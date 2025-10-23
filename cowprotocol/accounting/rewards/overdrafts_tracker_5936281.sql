-- This query tracks the events emitted by the overdrafts manager contract
-- https://github.com/cowprotocol/overdrafts-manager
-- and gives a view of the current state of overdrafts of all solvers for all chains
-- CoW Protocol operates on.
-- Note: an overdraft is the amount, expressed in the native token, a solver owes to the protocol
--       due to penalties, and/or negative slippage.

with overdraft_update_events as (
    select
        tx_hash,
        block_time,
        block_number,
        from_hex(lower(concat('0x', substr(to_hex(topic1), 25, 40)))) as solver,
        bytearray_to_uint256(bytearray_substring(data, 1, 32)) as old_overdraft,
        bytearray_to_uint256(bytearray_substring(data, 33, 32)) as new_overdraft
    from {{blockchain}}.logs
    where
        contract_address = 0x8fd67ea651329fd142d7cfd8e90406f133f26e8a
        and
        topic0 = 0x5e00297b9c4f8fef460c4d7123316eeeb08a64a0930d1d9cae0b61b7f663b254
        and
        block_time >= cast('2025-08-01' as timestamp)
),

most_recent_updates as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by solver
                order by block_number desc, block_time desc, tx_hash
            ) as rn
        from overdraft_update_events
    )
    where rn = 1
)

select
    s.environment,
    s.name,
    s.address as solver,
    s.active,
    coalesce(mru.new_overdraft, 0) as current_ovedraft_native_token_atoms,
    coalesce(mru.new_overdraft, 0) / pow(10,18) as current_overdraft_native_token_units
from cow_protocol_{{blockchain}}.solvers as s left join most_recent_updates as mru on s.address = mru.solver
order by coalesce(mru.new_overdraft, 0) desc
