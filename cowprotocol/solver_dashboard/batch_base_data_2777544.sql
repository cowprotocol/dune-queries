-- base query with data per batch
select 
    cast(block_deadline as int256) as block_deadline, 
    cast(block_number as int256) as block_number, -- Null here means the settlement did not occur.
    from_hex(solver) as winning_solver,
    from_hex(tx_hash) as tx_hash,
    -- Unpacking the data
    cast(cast(data.winning_score as varchar) as int256) as winning_score,
    cast(cast(data.reference_score as varchar) as int256) as reference_score,
    cast(cast(data.surplus as varchar) as int256) as surplus,
    cast(cast(data.protocol_fee as varchar) as int256) as protocol_fee,
    cast(cast(data.fee as varchar) as int256) as fee, -- network fee
    cast(cast(data.execution_cost as varchar) as int256) as execution_cost,
    cast(cast(data.uncapped_payment_eth as varchar) as int256) as uncapped_reward,
    cast(cast(data.capped_payment as varchar) as int256) as reward,
    transform(data.participating_solvers, x -> from_hex(x)) as participating_solvers,
    cardinality(data.participating_solvers) as num_participants
from cowswap.raw_batch_rewards