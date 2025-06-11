-- This query provides data related to rewards/payouts on a per auction level
-- for all auctions that had at least one winner.
-- Parameters:
-- : the chain for which we want to retrieve batch data

-- The output has the following columns:
--    environment: varchar
--    auction_id: integer
--    block_deadline: integer
--    solver: varbinary
--    competition_score: decimal(38, 0)
--    observed_score: decimal(38, 0)
--    uncapped_payment_native_token: decimal(38, 0)
--    capped_payment_native_token: decimal(38, 0)

select --noqa: ST06
    environment,
    auction_id,
    block_deadline,
    solver,
    sum(winning_score) as competition_score,
    sum(
        case
            when block_number is not null and block_number <= block_deadline then winning_score
            else 0
        end
    ) as observed_score,
    reference_score,
    uncapped_payment_native_token,
    capped_payment
from "query_4351957(blockchain='{{blockchain}}')"
group by
    environment,
    auction_id,
    block_deadline,
    solver,
    reference_score,
    uncapped_payment_native_token,
    capped_payment
