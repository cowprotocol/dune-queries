with polygon_unvouching as (
    select
        'polygon' as chain, --noqa: RF04
        contract_address,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        tx_to as evt_tx_to,
        null as evt_tx_index,
        index as evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        block_date as evt_block_date,
        from_hex(substr(cast(topic2 as varchar), 27, 40)) as bondingPool,
        from_hex(substr(cast(topic3 as varchar), 27, 40)) as sender,
        from_hex(substr(cast(topic1 as varchar), 27, 40)) as solver
    from polygon.logs
    where contract_address = 0xaaa4de096d02ae21729aa31d967e148d4e3ae501 and topic0 = 0xfbe946aa1fb3fabb46cdd9f88982f3d42ef58fad11ed89cb15456b9fe4ea7d7d
),

multichain_unvouching as (
    select *
    from cow_protocol_multichain.vouchregister_evt_invalidatevouch
    union all
    select *
    from polygon_unvouching
)

select
    contract_address,
    evt_tx_hash,
    evt_tx_from,
    evt_tx_to,
    evt_tx_index,
    evt_index,
    evt_block_time,
    evt_block_number,
    evt_block_date,
    bondingPool,
    sender,
    solver
from multichain_unvouching
where chain = '{{blockchain}}'
