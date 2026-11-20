-- This query provides data related to rewards/payouts on a per auction level on Solana
-- for all auctions that had at least one winner.

-- The output has the following columns:
--    environment: varchar
--    auction_id: integer
--    block_deadline: integer
--    solver: varbinary
--    total_protocol_fee: decimal(38, 0)
--    competition_score: decimal(38, 0)
--    observed_score: decimal(38, 0)
--    uncapped_payment_native_token: decimal(38, 0)
--    capped_payment: decimal(38, 0)

with slot_range as (
    select
        min(slot) as start_slot,
        max(slot) as end_slot
    from solana.blocks
    where time >= cast('{{start_time}}' as timestamp) - interval '1' day
      and time <  cast('{{end_time}}' as timestamp) + interval '1' day
      -- partition pruning; solana.blocks is huge
      and date >= cast(cast('{{start_time}}' as timestamp) as date) - interval '1' day
      and date <= cast(cast('{{end_time}}' as timestamp) as date) + interval '1' day
),

relevant_batches as (
    select *
    from dune.cowprotocol.solana_batch_data_view
    where
        deadline_slot >= (select start_slot from slot_range)
        and
        deadline_slot <= (select end_slot from slot_range)
)

select --noqa: ST06
    environment,
    auction_id,
    deadline_slot,
    solver,
    cast(sum(coalesce(protocol_fee, 0)) as decimal(38, 0)) as total_protocol_fee,
    cast(sum(winning_score) as decimal(38, 0)) as competition_score,
    cast(sum(
        case
            when slot is not null and slot <= deadline_slot then winning_score
            else 0
        end
    ) as decimal(38, 0)) as observed_score,
    cast(reference_score as decimal(38, 0)) as reference_score,
    cast(uncapped_payment_native_token as decimal(38, 0)) as uncapped_payment_native_token,
    cast(capped_payment as decimal(38, 0)) as capped_payment
from relevant_batches
group by
    environment,
    auction_id,
    deadline_slot,
    solver,
    -- the last three columns for grouping are generated per auction and solver, not per solution
    -- the group by ensures that we do not double count these entries but just select them
    reference_score,
    uncapped_payment_native_token,
    capped_payment
