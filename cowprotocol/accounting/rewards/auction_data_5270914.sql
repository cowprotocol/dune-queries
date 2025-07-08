-- This query provides data related to rewards/payouts on a per auction level
-- for all auctions that had at least one winner.
-- Parameters:
--   blockchain: the chain for which we want to retrieve batch data

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

txs_block_range as (
    select
        min(block_number) as first_block,
        max(block_number) as last_block
    from "query_4351957(blockchain='{{blockchain}}')"
    where block_deadline >= (select start_block from block_range) and block_deadline <= (select end_block from block_range)
),

block_data as (
    select
        tx.first_block,
        min_block.date as min_block_date,
        tx.last_block,
        max_block.date as max_block_date
    from txs_block_range as tx
        inner join {{blockchain}}.blocks as max_block
            on tx.last_block = max_block.number
        inner join {{blockchain}}.blocks as min_block
            on tx.last_block = min_block.number
),

-- the following table is a restriction of the transactions table, with the goal to speed up subsequent computations
candidate_txs as (
    select *
    from {{blockchain}}.transactions
    where block_date >= (select min_block_date from block_data) and block_date <= (select max_block_date from block_data)
        and block_number >= (select first_block from txs_block_range) and block_number <= (select last_block from txs_block_range)
),

relevant_txs as (
    select
        t.hash as tx_hash,
        t.gas_price * t.gas_used as execution_cost
    from "query_4351957(blockchain='{{blockchain}}')" as rbd inner join candidate_txs as t
        on rbd.block_number = t.block_number and rbd.tx_hash = t.hash
    where block_deadline >= (select start_block from block_range) and block_deadline <= (select end_block from block_range)
)

select --noqa: ST06
    rbd.environment,
    rbd.auction_id,
    rbd.block_deadline,
    rbd.solver,
    cast(sum(coalesce(rbd.network_fee, 0)) as decimal(38, 0)) as total_network_fee,
    cast(sum(coalesce(txs.execution_cost, 0)) as decimal(38, 0)) as total_execution_cost,
    cast(sum(coalesce(rbd.protocol_fee, 0)) as decimal(38, 0)) as total_protocol_fee,
    cast(sum(rbd.winning_score) as decimal(38, 0)) as competition_score,
    cast(sum(
        case
            when rbd.block_number is not null and rbd.block_number <= rbd.block_deadline then winning_score
            else 0
        end
    ) as decimal(38, 0)) as observed_score,
    cast(rbd.reference_score as decimal(38, 0)) as reference_score,
    cast(rbd.uncapped_payment_native_token as decimal(38, 0)) as uncapped_payment_native_token,
    cast(rbd.capped_payment as decimal(38, 0)) as capped_payment
from "query_4351957(blockchain='{{blockchain}}')" as rbd
left join relevant_txs as txs on rbd.tx_hash = txs.tx_hash
where
    rbd.block_deadline >= (select start_block from block_range)
    and rbd.block_deadline <= (select end_block from block_range)
group by
    rbd.environment,
    rbd.auction_id,
    rbd.block_deadline,
    rbd.solver,
    -- the last three columns for grouping are generated per auction and solver, not per solution
    -- the group by ensures that we do not double count these entries but just select them
    rbd.reference_score,
    rbd.uncapped_payment_native_token,
    rbd.capped_payment
