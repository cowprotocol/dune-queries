-- base query with data per batch
select --noqa: ST06
    cast(block_deadline as int256) as block_deadline,
    cast(block_number as int256) as block_number, -- Null here means the settlement did not occur.
    from_hex(solver) as winning_solver, --noqa: RF03
    from_hex(tx_hash) as tx_hash,
    -- Unpacking the data
    cast(cast(data.winning_score as varchar) as int256) as winning_score, --noqa: RF01, RF03
    cast(cast(data.reference_score as varchar) as int256) as reference_score, --noqa: RF01
    cast(cast(data.surplus as varchar) as int256) as surplus, --noqa: RF01
    cast(cast(data.protocol_fee as varchar) as int256) as protocol_fee, --noqa: RF01
    cast(cast(data.fee as varchar) as int256) as fee, -- network fee  --noqa: RF01
    cast(cast(data.execution_cost as varchar) as int256) as execution_cost,  --noqa: RF01
    cast(cast(data.uncapped_payment_eth as varchar) as int256) as uncapped_reward,  --noqa: RF01
    cast(cast(data.capped_payment as varchar) as int256) as reward,  --noqa: RF01
    transform(data.participating_solvers, x -> from_hex(x)) as participating_solvers,  --noqa: RF01, RF03, PRS
    cardinality(data.participating_solvers) as num_participants --noqa: RF01
from cowswap.raw_batch_rewards
