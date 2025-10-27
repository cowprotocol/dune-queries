with polygon_vouching as (
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
        from_hex(substr(cast(data as varchar), 27, 40)) as cowRewardTarget,
        from_hex(substr(cast(topic3 as varchar), 27, 40)) as sender,
        from_hex(substr(cast(topic1 as varchar), 27, 40)) as solver
    from polygon.logs
    where contract_address = 0xaaa4de096d02ae21729aa31d967e148d4e3ae501 and topic0 = 0xd30c692ff1e6e1e96d8aca701b7f8118d58f64ce4c680feda75c0fc76524f7fa
),

bnb_vouching as (
    select
        'bnb' as chain, --noqa: RF04
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
        from_hex(substr(cast(data as varchar), 27, 40)) as cowRewardTarget,
        from_hex(substr(cast(topic3 as varchar), 27, 40)) as sender,
        from_hex(substr(cast(topic1 as varchar), 27, 40)) as solver
    from bnb.logs
    where contract_address = 0xaaa4de096d02ae21729aa31d967e148d4e3ae501 and topic0 = 0xd30c692ff1e6e1e96d8aca701b7f8118d58f64ce4c680feda75c0fc76524f7fa
),

multichain_vouching as (
    select *
    from cow_protocol_multichain.vouchregister_evt_vouch
    union all
    select *
    from polygon_vouching
    union all
    select *
    from bnb_vouching
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
    cowRewardTarget,
    sender,
    solver
from multichain_vouching
where chain = '{{blockchain}}'
