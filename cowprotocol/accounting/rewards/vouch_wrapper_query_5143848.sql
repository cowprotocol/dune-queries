with polygon_vouching as (
    select
        'polygon' as chain, --noqa: RF04
        contract_address,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        tx_to as evt_tx_to,
        null as evt_tx_index,
        evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        block_date as evt_block_date,
        bonding_pool as bondingPool, --noqa: CP02
        cow_reward_target as cowRewardTarget, --noqa: CP02
        sender,
        solver
    from dune.cowprotocol.solver_vouch_events
    where blockchain = 'polygon' and event_type = 'vouch'
),

bnb_vouching as (
    select
        'bnb' as chain, --noqa: RF04
        contract_address,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        tx_to as evt_tx_to,
        null as evt_tx_index,
        evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        block_date as evt_block_date,
        bonding_pool as bondingPool, --noqa: CP02
        cow_reward_target as cowRewardTarget, --noqa: CP02
        sender,
        solver
    from dune.cowprotocol.solver_vouch_events
    where blockchain = 'bnb' and event_type = 'vouch'
),
linea_vouching as (
    select
        'linea' as chain, --noqa: RF04
        contract_address,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        tx_to as evt_tx_to,
        null as evt_tx_index,
        evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        block_date as evt_block_date,
        bonding_pool as bondingPool, --noqa: CP02
        cow_reward_target as cowRewardTarget, --noqa: CP02
        sender,
        solver
    from dune.cowprotocol.solver_vouch_events
    where blockchain = 'linea' and event_type = 'vouch'
),
plasma_vouching as (
    select
        'plasma' as chain, --noqa: RF04
        contract_address,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        tx_to as evt_tx_to,
        null as evt_tx_index,
        evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        block_date as evt_block_date,
        bonding_pool as bondingPool, --noqa: CP02
        cow_reward_target as cowRewardTarget, --noqa: CP02
        sender,
        solver
    from dune.cowprotocol.solver_vouch_events
    where blockchain = 'plasma' and event_type = 'vouch'
),
multichain_vouching as (
    select *
    from cow_protocol_multichain.vouchregister_evt_vouch
    union distinct
    select *
    from polygon_vouching
    union distinct
    select *
    from bnb_vouching
    union distinct
    select *
    from linea_vouching
    union distinct
    select *
    from plasma_vouching
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
    bondingPool, --noqa: CP02
    cowRewardTarget, --noqa: CP02
    sender,
    solver
from multichain_vouching
where chain = '{{blockchain}}'
