-- This query is the basis for experiments with different rewards mechanisms
--
-- It is under version control in https://github.com/cowprotocol/dune-queries
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--  {{scaling}} - fraction of protocol fees used to cap rewards
--  {{price_improvement_fee}} - fraction of price improvement charged as fee
--  {{volume_fee_bps}} - fraction of volume charged as fee
--  {{fixed_fee}} - additional fixed fee per trade
--
-- The columns of the result are
-- - time: time of the auction (deadline)
-- - auction_id: id of the auction
-- - solver: winning solver in that auction
-- - solver_name: dune name of the solver
-- - protocol_fee: sum of protocol fees charged by a solver, in native token
-- - volume: sum of volume of trades, in native token
-- - new_protocol_fee: protocol fee when charging price improvement, volume and fixed fee
-- - uncapped_reward: uncapped second price reward for the solver in that auction
-- - reward: current capped reward using caps per chain
-- - new_reward_proposal: rewards discussed in first post of https://forum.cow.fi/t/cip-draft-align-solver-rewards-with-protocol-revenue/3174
-- - new_reward_fee_cap: reward based on capping from above by a fraction of new_protocol_fee, the original cap from below applies


with batch_data as (
    select
        b.time,
        rbd.auction_id,
        rbd.solver,
        s.name as solver_name,
        rbd.uncapped_payment_native_token as uncapped_reward,
        rbd.capped_payment as reward,
        count(*) as number_of_trades,
        sum((rod.protocol_fee - coalesce(rod.partner_fee, 0)) * rod.protocol_fee_native_price) as protocol_fee, -- this is the actual revenue of the protocol
        sum(case when t.order_type = 'SELL' then atoms_bought * rod.protocol_fee_native_price else atoms_sold * rod.protocol_fee_native_price end) as volume
    from "query_4351957(blockchain='{{blockchain}}')" as rbd
    join "query_4364122(blockchain='{{blockchain}}')" as rod
        on rbd.auction_id = rod.auction_id and rbd.solver = rod.solver and rbd.tx_hash = rod.tx_hash
    join cow_protocol_{{blockchain}}.trades as t
        on rod.order_uid = t.order_uid and rod.tx_hash = t.tx_hash
    join {{blockchain}}.blocks as b
        on rbd.block_deadline = b.number
    join cow_protocol_{{blockchain}}.solvers as s
        on rbd.solver = s.address
    where b.time >= (timestamp '{{start_time}}') and b.time < (timestamp '{{end_time}}')
    group by 1, 2, 3, 4, 5, 6
),

caps as (
    select
        max(reward) as upper_cap,
        min(reward) as lower_cap
    from batch_data
),

rewards_per_auction as (
    select
        time,
        auction_id,
        solver,
        solver_name,
        protocol_fee,
        volume,
        {{price_improvement_fee}} * 2 * protocol_fee + volume * {{volume_fee_bps}} / 1e4 + {{fixed_fee}} * 1e18 * number_of_trades as new_protocol_fee,
        uncapped_reward,
        reward
    from batch_data
    where
        volume < 1e23 -- this was for filtering out auctions with obvious bogus volume
        and volume > 2 * uncapped_reward -- this was filtering for auctions with obvious bogus reward
)

select
    *,
    greatest(reward, 0.5 * protocol_fee) as  new_reward_proposal,
    least({{scaling}} * new_protocol_fee, greatest((select lower_cap from caps), uncapped_reward)) as new_reward_fee_cap
from rewards_per_auction
