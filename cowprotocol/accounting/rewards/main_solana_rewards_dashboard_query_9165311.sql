-- Main Solana Payouts Query. Currently only working for the prod settlement program
with

auction_range as (
    select
        environment,
        min(auction_id) as min_auction_id,
        max(auction_id) as max_auction_id
    from "query_9163100(start_time='{{start_time}}',end_time='{{end_time}}')"
    group by environment
),

solver_slippage as (
    select
        solver_address as solver,
        slippage_lamports * 1.0 / pow(10, 9) as slippage
    from "query_9163394(start_time='{{start_time}}',end_time='{{end_time}}',slippage_table_name='slippage_per_solver')"
),

named_results as (
    select * from "query_8995280(end_time='{{end_time}}')"
),

-- BEGIN SOLVER REWARDS
auction_data as (
    select
        ad.environment,
        ad.auction_id,
        ad.solver,
        ad.capped_payment
    from "query_9163100(start_time='{{start_time}}',end_time='{{end_time}}')" as ad
    inner join auction_range on ad.environment = auction_range.environment
    where
        ad.auction_id >= auction_range.min_auction_id
        and ad.auction_id <= auction_range.max_auction_id
        and ad.environment = 'prod'
),

-- AKA Performance Rewards
primary_rewards as (
    select
        solver,
        cast(sum(capped_payment) as double) as reward_lamports
    from auction_data
    group by solver
),

consistency_rewards_all_periods as (
    select
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        solver,
        consistency_reward_native
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_solana
),

consistency_rewards as (
    select
        solver,
        cast(consistency_reward_native as double) as consistency_reward_lamports
    from consistency_rewards_all_periods
    where
        accounting_period_start_time = cast('{{start_time}}' as timestamp)
        and accounting_period_end_time = cast('{{end_time}}' as timestamp)
),

solver_competition_rewards as (
    select
        coalesce(a.solver, b.solver) as solver,
        coalesce(a.reward_lamports, 0) as performance_reward_lamports,
        coalesce(b.consistency_reward_lamports, 0) as consistency_reward_lamports,
        coalesce(a.reward_lamports, 0) + coalesce(b.consistency_reward_lamports, 0) as reward_lamports
    from primary_rewards as a
    full outer join consistency_rewards as b on a.solver = b.solver
),

conversion_prices as (
    select
        cow_price,
        native_token_price
    from dune.cowprotocol.result_accounting_cow_and_native_prices_per_chain
    where
        blockchain = 'solana'
        and end_time > date_add('day', -1, cast('{{end_time}}' as timestamp))
        and end_time < date_add('day', +1, cast('{{end_time}}' as timestamp))
),

aggregate_results as (
    select
        solver,
        reward_lamports / pow(10, 9) as primary_reward_sol,
        performance_reward_lamports / pow(10, 9) as performance_reward_sol,
        consistency_reward_lamports / pow(10, 9) as consistency_reward_sol,
        reward_lamports / pow(10, 9) * (select native_token_price / cow_price from conversion_prices) as primary_reward_cow,
        performance_reward_lamports / pow(10, 9) * (select native_token_price / cow_price from conversion_prices) as performance_reward_cow,
        consistency_reward_lamports / pow(10, 9) * (select native_token_price / cow_price from conversion_prices) as consistency_reward_cow
    from solver_competition_rewards
),

solvers as (
    select
        address_text as address,
        environment,
        name,
        whitelisted as active
    from dune.cowprotocol.solvers
    where blockchain = 'solana'
),

combined_data as (
    select
        coalesce(ar.solver, ss.solver) as solver,
        coalesce(ar.primary_reward_sol, 0) as primary_reward_sol,
        coalesce(ar.primary_reward_cow, 0) as primary_reward_cow,
        coalesce(ar.performance_reward_cow, 0) as performance_reward_cow,
        coalesce(ar.consistency_reward_cow, 0) as consistency_reward_cow,
        cast(0 as double) as quote_reward,        -- quote rewards disabled
        coalesce(ss.slippage, 0) as slippage_sol,
        concat(s.environment, '-', s.name) as name  --noqa: RF04
    from aggregate_results as ar
    full outer join solver_slippage as ss
        on ar.solver = ss.solver
    left join solvers as s
        on coalesce(ar.solver, ss.solver) = s.address
),

extended_payout_data as (
    select --noqa: ST06
        cd.*,
        cd.primary_reward_sol + cd.slippage_sol as total_outgoing_sol,
        coalesce(cd.primary_reward_sol + cd.slippage_sol < 0, false) as is_overdraft,
        cd.slippage_sol as reimbursement_sol,
        cd.slippage_sol * (select native_token_price / cow_price from conversion_prices) as reimbursement_cow,
        cd.primary_reward_cow as total_cow_reward,
        cd.primary_reward_sol as total_sol_reward
    from combined_data as cd
)

select  --noqa: ST06
    epd.name,
    epd.solver as solver_address,
    nr.cow_reward_target,
    nr.solana_reward_target,
    epd.quote_reward,
    case
        when is_overdraft then null
        when reimbursement_sol > 0 and total_cow_reward < 0
            then reimbursement_sol + total_sol_reward
        when reimbursement_sol < 0 and total_cow_reward > 0
            then 0
        else reimbursement_sol
    end as native_token_transfer,
    case
        when is_overdraft then null
        when reimbursement_sol > 0 and total_cow_reward < 0
            then 0
        when reimbursement_sol < 0 and total_cow_reward > 0
            then reimbursement_cow + total_cow_reward
        else total_cow_reward
    end as cow_transfer,
    case
        when is_overdraft then total_outgoing_sol
    end as overdraft,
    slippage_sol as slippage_native_token,
    reimbursement_sol as reimbursement_native_token,
    reimbursement_cow,
    total_cow_reward,
    performance_reward_cow,
    consistency_reward_cow
from extended_payout_data as epd
left join named_results as nr on epd.solver = nr.solver
