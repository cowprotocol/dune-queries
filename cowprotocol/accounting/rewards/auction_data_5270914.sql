-- This query provides data related to rewards/payouts on a per auction level
-- for all auctions that had at least one winner.
-- Parameters:
-- : the chain for which we want to retrieve batch data

-- The output has the following columns:
--    environment: varchar
--    auction_id: integer
--    block_deadline: integer
--    solver: varbinary
--    total_network_fee: decimal(38, 0)
--    total_execution_cost: decimal(38, 0)
--    total_protocol_fee: decimal(38, 0)
--    competition_score: decimal(38, 0)
--    observed_score: decimal(38, 0)
--    uncapped_payment_native_token: decimal(38, 0)
--    capped_payment_native_token: decimal(38, 0)

with block_range as (
    select * from "query_3333356(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

relevant_txs as (
    select
        rbd.environment,
        rbd.auction_id,
        rbd.block_deadline,
        rbd.solver,
        rbd.tx_hash,
        t.gas_price * t.gas_used as execution_cost
    from "query_4351957(blockchain='{{blockchain}}')" as rbd inner join {{blockchain}}.transactions as t
        on rbd.tx_hash = t.hash
    where block_deadline >= (select start_block from block_range) and block_deadline <= (select end_block from block_range)
)

select --noqa: ST06
    rbd.environment,
    rbd.auction_id,
    rbd.block_deadline,
    rbd.solver,
    sum(rbd.network_fee) as total_network_fee,
    sum(coalesce(rbd.execution_cost, txs.execution_cost, 0)) as total_execution_cost,
    sum(rbd.protocol_fee) as total_protocol_fee,
    sum(rbd.winning_score) as competition_score,
    sum(
        case
            when rbd.block_number is not null and rbd.block_number <= rbd.block_deadline then winning_score
            else 0
        end
    ) as observed_score,
    rbd.reference_score,
    rbd.uncapped_payment_native_token,
    rbd.capped_payment
from "query_4351957(blockchain='{{blockchain}}')" as rbd left join relevant_txs as txs
    on rbd.environment = txs.environment and rbd.auction_id = txs.auction_id and rbd.block_deadline = txs.block_deadline and rbd.solver = txs.solver
where rbd.block_deadline >= (select start_block from block_range) and rbd.block_deadline <= (select end_block from block_range)
group by
    rbd.environment,
    rbd.auction_id,
    rbd.block_deadline,
    rbd.solver,
    rbd.reference_score,
    rbd.uncapped_payment_native_token,
    rbd.capped_payment
