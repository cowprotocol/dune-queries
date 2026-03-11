-- This query experiments with a consistency rewards mechanism for CoW Protocol solver rewards.
-- It compares the current reward system with a proposed split into performance rewards
-- (capped at fee_cap * protocol_fee per auction) and consistency rewards (distributed from
-- the remaining budget proportionally to a chosen consistency metric).
--
-- Parameters:
--  {{blockchain}}          - network to run the analysis on (e.g. 'ethereum', 'gnosis')
--  {{start_time}}          - start of the time range (inclusive) for auction data
--  {{end_time}}            - end of the time range (exclusive) for auction data
--  {{fee_cap}}             - fraction of protocol fee used as the per-auction reward cap
--                            (e.g. 0.5 means at most 50% of the protocol fee per auction
--                            goes to the winning solver as performance reward; remainder
--                            flows into the weekly consistency budget)
--  {{consistency_metric}}  - CTE name selecting which metric distributes the consistency
--                            budget; one of:
--                              metric_nr_orders    - number of distinct orders proposed
--                              metric_top4_orders  - weighted count of orders where the
--                                                    solver appeared in the top 4 by surplus
--                                                    (weight = 12 / competing_solvers_in_top4)
--                              metric_robust_surplus - marginal contribution to
--                                                    surplus under a probabilistic model
--                                                    (see below for details)
--  {{solver}}              - a specific solver address to apply volume filtering to
--  {{volume_filter}}       - minimum total volume (in ETH) required for an auction of
--                            {{solver}} to be included; auctions below this threshold are
--                            excluded (see filtering notes below)
--
-- Consistency budget:
--  For each auction and solver, the available budget is fee_cap * protocol_fee. It is split as follows:
--    Positive rewards (uncapped_reward > 0):
--      performance_reward = min(uncapped_reward, fee_cap * protocol_fee)
--      consistency_budget = max(fee_cap * protocol_fee - uncapped_reward, 0)
--    Negative rewards/Penalties (uncapped_reward <= 0):
--      performance_reward = reward  (actual negative reward, passed through unchanged)
--      consistency_budget = fee_cap * protocol_fee  (full budget goes to consistency)
--  In auctions with penaltis the budget increases by the cap and not additionally due to the
--  penalty. It is possible that the uncapped reward is negative and that the protocol fee is
--  positive, e.g., when a solver wins with multiple solutions but not all of them settle on chain.
--
-- Consistency metrics explained ({{consistency_metric}} selects among these):
--  All metrics are anchored to executed orders — orders that actually settled on-chain
--  in the selected time range — and only consider solutions that were not filtered out
--  (proposed_solutions_filtered, filtered_out = false).
--
--  metric_nr_orders:
--    Each solver's metric equals the number of distinct executed orders they included in
--    an accepted, non-filtered-out solution.
--    Rewards breadth of participation.
--
--  metric_top4_orders:
--    For each executed order, solvers are ranked by the surplus they offered (surplus =
--    executed_buy minus the limit-rate-adjusted buy amount). The top-4 solvers for each
--    order split 12 points equally: each earns 12 / (number of solvers in top 4).
--    Example: solvers A, B, C are all in the top 4 for an order → each earns 12/3 = 4.
--    If only solver A qualifies → A earns 12.
--    Rewards quality: solvers that are competitive across many orders.
--
--  metric_robust_surplus:
--    Estimates each solver's marginal contribution to expected settled surplus, assuming
--    solvers participate independently with their observed participation rate.
--    Only SELL orders are considered.
--
--    Setup: solver j's participation rate p_j = (distinct SELL orders they proposed in
--    accepted solutions) / (total distinct executed SELL orders in the week). For each
--    order, solvers are ranked by their offered surplus (rank 1 = highest, s_1 >= s_2 >= ...).
--
--    Raw contribution of solver j at rank k:
--      C_j = remaining_prob_j * p_j * s_j
--    where remaining_prob_j = prod_{i=1}^{k-1} (1 - p_i) is the probability that none of
--    the k-1 better solvers participates. C_j is the expected surplus j delivers when they
--    are the best available solver for this order.
--
--    Marginal contribution of j = E[surplus with j] - E[surplus without j].
--    Removing j from rank k increases every worse solver i's remaining_prob by a factor of
--    1/(1-p_j), because the (1-p_j) term disappears from their product. The surplus gained
--    by solver i when j is absent is therefore p_j/(1-p_j) * C_i. Summing over all worse
--    solvers gives the closed-form formula computed by the `marginal_contributions` CTE:
--      MC_j = C_j - p_j/(1-p_j) * sum_{i: rank(i) > rank(j)} C_i
--    This avoids summing over exponentially many participant subsets.
--
--    Example (three solvers, one order):
--      A: rank 1, s=10, p=0.90 → remaining_prob = 1.0,              C_A = 9.0
--      B: rank 2, s= 9, p=0.80 → remaining_prob = 1-0.9 = 0.1,     C_B = 0.72
--      C: rank 3, s= 8, p=0.95 → remaining_prob = 0.1*(1-0.8)=0.02, C_C = 0.152
--      expected surplus = C_A + C_B + C_C = 9.872
--      MC_A = 9.0  - (0.9/0.1) * (0.72 + 0.152) = 1.152
--      MC_B = 0.72 - (0.8/0.2) * 0.152           = 0.112
--      MC_C = 0.152                               = 0.152
--      Note: C has the highest participation rate but ranks last by surplus, so despite
--      p_C > p_A, most of C's surplus is already covered by A and B being present.
--    A solver's metric is the sum of MC across all orders.
--    Rewards solvers who generate surplus that would not be available without them.
--
-- Filtering ({{solver}} / {{volume_filter}}):
--  Auctions where {{solver}} participated with a total volume below {{volume_filter}} ETH
--  are excluded from both the performance data and the proposed solutions. This reduces
--  {{solver}}'s performance rewards and their metric contribution, but does not redistribute
--  the excluded rewards to other solvers (the consistency budget also shrinks accordingly).
--  The filter is intended to study the effect of ignoring low-volume auctions for a solver.
--  Rewards for other solvers are not accurate when filtering is enabled.
--
-- Output columns (aggregated per solver over the selected time range):
--  solver              - solver address
--  current_reward      - total reward under the current mechanism (ETH)
--  new_reward          - total reward under the proposed mechanism:
--                        performance_reward + consistency_reward (ETH)
--  performance_reward  - capped performance reward: min(uncapped_reward, fee_cap *
--                        protocol_fee) per auction, summed over all winning auctions;
--                        negative rewards (losing auctions) are passed through unchanged (ETH)
--  consistency_reward  - share of the weekly consistency budget allocated proportionally
--                        to the solver's consistency metric (ETH)
--  protocol_fee        - total protocol fees generated by the solver's settlements (ETH)
--  volume              - total settlement volume (ETH)

with performance_data_per_auction as (
    select
        auction_time,
        accounting_week_start,
        auction_id,
        solver,
        reward as current_reward,
        volume,
        protocol_fee,
        case when uncapped_reward <= 0 then reward else least(uncapped_reward, {{fee_cap}} * protocol_fee) end as performance_reward,
        case when uncapped_reward <= 0 then {{fee_cap}} * protocol_fee else greatest({{fee_cap}} * protocol_fee - uncapped_reward, 0) end as consistency_budget
    from dune.cowprotocol.result_performance_data_per_auction_oct_2025_feb_2026
    where
        blockchain = '{{blockchain}}'
        and auction_time >= (timestamp '{{start_time}}') and auction_time < (timestamp '{{end_time}}')
),

-- the next cte filters out all performance data for {{solver}} if the total reward was positive and the volume of executed orders was smaller than {{volume_filter}}
performance_data_per_auction_filtered as (
    select * from performance_data_per_auction
    where solver != {{solver}} or current_reward < 0 or volume / 1e18 >= {{volume_filter}}
),

performance_rewards as (
    select
        accounting_week_start,
        solver,
        sum(current_reward) as current_reward,
        sum(performance_reward) as performance_reward,
        sum(consistency_budget) as consistency_budget,
        sum(volume) as volume,
        sum(protocol_fee) as protocol_fee
    from performance_data_per_auction_filtered
    group by 1, 2
),

data_per_trade as (
    select
        block_time as auction_time,
        date_add('day', -((day_of_week(date_trunc('day', block_time)) + 5) % 7), date_trunc('day', block_time)) as accounting_week_start,
        rod.auction_id,
        rod.order_uid,
        t.order_type,
        t.limit_sell_amount,
        t.limit_buy_amount,
        rod.protocol_fee_native_price,
        case when t.order_type = 'SELL' then t.atoms_bought * rod.protocol_fee_native_price else t.atoms_sold * rod.protocol_fee_native_price end as volume
    from cow_protocol_{{blockchain}}.trades as t
    inner join "query_4364122(blockchain='{{blockchain}}')" as rod on t.order_uid = rod.order_uid and t.tx_hash = rod.tx_hash
    inner join performance_data_per_auction as pd on rod.auction_id = pd.auction_id
),

proposed_solutions as (
    select * from query_6741796
    where blockchain = '{{blockchain}}'
),

proposed_trade_executions as (
    select * from query_6741982
    where blockchain = '{{blockchain}}'
),

-- the next cte filters out all proposed solutions for {{solver}} if the volume of executed orders included in the proposed solution was smaller than {{volume_filter}}
proposed_solutions_filtered as (
    select ps.*
    from proposed_solutions as ps
    where
        ps.solver != {{solver}}
        or (
            select sum(td.volume / 1e18) >= {{volume_filter}}
            from proposed_trade_executions as pte
            inner join data_per_trade as td on pte.order_uid = td.order_uid
            where pte.auction_id = ps.auction_id and pte.solution_uid = ps.solution_uid
        )
),

metric_nr_orders as (
    select
        t.accounting_week_start,
        ps.solver,
        count(distinct t.order_uid) as metric
    from data_per_trade as t
    inner join proposed_trade_executions as pte on t.order_uid = pte.order_uid
    inner join proposed_solutions_filtered as ps on pte.auction_id = ps.auction_id and pte.solution_uid = ps.solution_uid
    where ps.filtered_out = false
    group by 1, 2
),

surplus_per_order as (
    select
        t.accounting_week_start,
        ps.auction_id,
        ps.solver,
        t.order_uid,
        t.order_type,
        max(pte.executed_buy - t.limit_buy_amount * pte.executed_sell / t.limit_sell_amount) as surplus,
        max((pte.executed_buy - t.limit_buy_amount * pte.executed_sell / t.limit_sell_amount) * t.protocol_fee_native_price) as surplus_native,
        rank() over (
            partition by t.order_uid
            order by max(pte.executed_buy - t.limit_buy_amount * pte.executed_sell / t.limit_sell_amount) desc
        ) as rk
    from data_per_trade as t
    inner join proposed_trade_executions as pte on t.order_uid = pte.order_uid
    inner join proposed_solutions_filtered as ps on pte.auction_id = ps.auction_id and pte.solution_uid = ps.solution_uid
    where ps.filtered_out = false
    group by 1, 2, 3, 4, 5
),

metric_top4_orders as (
    select
        accounting_week_start,
        solver,
        sum(12.0 / solvers_on_order) as metric
    from (
        select
            accounting_week_start,
            solver,
            order_uid,
            count(*) over (partition by order_uid) as solvers_on_order
        from surplus_per_order
        where rk <= 4
    )
    group by 1, 2
),

participation_rates as (
    select
        accounting_week_start,
        solver,
        cast(count(distinct order_uid) as double) / max(total_orders) as participation_rate
    from (
        select
            accounting_week_start,
            solver,
            order_uid,
            count(distinct order_uid) over (partition by accounting_week_start) as total_orders
        from surplus_per_order
        where order_type = 'SELL'
    )
    group by 1, 2
),

robust_surplus_per_bid as (
    select
        spo.accounting_week_start,
        spo.solver,
        spo.order_uid,
        spo.surplus_native,
        p.participation_rate,
        spo.rk,
        coalesce(
            exp(sum(ln(1.0 - p.participation_rate)) over (
                partition by spo.order_uid
                order by spo.rk
                rows between unbounded preceding and 1 preceding
            )),
            1.0
        ) as remaining_prob
    from surplus_per_order as spo
    inner join participation_rates as p on spo.accounting_week_start = p.accounting_week_start and spo.solver = p.solver
    where spo.order_type = 'SELL'
),

marginal_contributions as (
    select
        accounting_week_start,
        solver,
        order_uid,
        remaining_prob * participation_rate * surplus_native as contribution,
        remaining_prob * participation_rate * surplus_native
        - participation_rate / (1.0 - participation_rate)
        * coalesce(sum(remaining_prob * participation_rate * surplus_native) over (
            partition by order_uid
            order by rk
            rows between 1 following and unbounded following
        ), 0) as marginal_contribution
    from robust_surplus_per_bid
),

metric_robust_surplus as (
    select
        accounting_week_start,
        solver,
        sum(marginal_contribution) as metric
    from marginal_contributions
    group by 1, 2
),

consistency_budget as (
    select
        accounting_week_start,
        sum(consistency_budget) as consistency_budget
    from performance_rewards
    group by 1
),

consistency_metric_normalization as (
    select
        accounting_week_start,
        sum(metric) as metric
    from {{consistency_metric}}
    group by 1
),

consistency_reward_per_unit as (
    select
        cb.accounting_week_start,
        consistency_budget / metric as consistency_reward_per_unit
    from consistency_budget as cb
    join consistency_metric_normalization as cmn on cb.accounting_week_start = cmn.accounting_week_start
),

consistency_rewards as (
    select
        cm.accounting_week_start,
        solver,
        consistency_reward_per_unit * metric as consistency_reward
    from {{consistency_metric}} as cm
    join consistency_reward_per_unit as crpu on cm.accounting_week_start = crpu.accounting_week_start
)

select
    pr.solver,
    sum(current_reward / 1e18) as current_reward,
    sum((performance_reward + consistency_reward) / 1e18) as new_reward,
    sum(performance_reward / 1e18) as performance_reward,
    sum(consistency_reward / 1e18) as consistency_reward,
    sum(protocol_fee / 1e18) as protocol_fee,
    sum(volume / 1e18) as volume
from performance_rewards as pr
join consistency_rewards as cr on pr.accounting_week_start = cr.accounting_week_start and pr.solver = cr.solver
group by 1
order by 2 desc
