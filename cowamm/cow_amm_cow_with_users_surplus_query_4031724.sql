-- This query computes how much surplus has been provided to CoW AMMs, when trading with other user orders
-- as part of a CoW. For that, a CoW detector query is used (4025739(). Finally, the query computes the 
-- distribution of an amount {{cow_budget}} of COW tokens to solvers, proportionally to the surplus generated 
-- via CoWs and pushed to CoW AMMs.
-- Parameters:
--  {{start_time}} - the start date timestamp for the accounting period  (inclusively)
--  {{end_time}} - the end date timestamp for the accounting period (exclusively)
-- {{cow_budget}} -- the amount of COW that needs to be distributed

with cow_amm_surplus as (
    select
        tx_hash,
        case
            when token_1_transfer_usd > 0 then token_1_transfer_usd + (token_1_balance_usd - token_1_transfer_usd) * token_2_transfer_usd / token_2_balance_usd
            else token_2_transfer_usd + (token_2_balance_usd - token_2_transfer_usd) * token_1_transfer_usd / token_1_balance_usd
        end as surplus
    from dune.cowprotocol.result_balancer_cow_amm_base_query_v_2
    where istrade
),

cow_surplus_per_batch_ethereum as (
    select
        cow_per_batch.block_time,
        cow_per_batch.tx_hash,
        solver_address,
        naive_cow,  -- fraction of batch volume traded within a CoW
        surplus as surplus_in_usd,  -- surplus of the executed CoW AMM order, expressed in USD
        naive_cow * surplus as realized_cow_surplus_in_usd -- surplus of the CoW AMM that is assumed to be generated via a CoW.
    from "query_4025739(blockchain='ethereum',start_time='{{start_time}}',end_time='{{end_time}}')" as cow_per_batch
    inner join cow_amm_surplus on cow_per_batch.tx_hash = cow_amm_surplus.tx_hash
    inner join cow_protocol_ethereum.batches as b on cow_per_batch.tx_hash = b.tx_hash
),

cow_surplus_per_batch_gnosis as (
    select
        cow_per_batch.block_time,
        cow_per_batch.tx_hash,
        solver_address,
        naive_cow,  -- fraction of batch volume traded within a CoW
        surplus as surplus_in_usd,  -- surplus of the executed CoW AMM order, expressed in USD
        naive_cow * surplus as realized_cow_surplus_in_usd -- surplus of the CoW AMM that is assumed to be generated via a CoW.
    from "query_4025739(blockchain='gnosis',start_time='{{start_time}}',end_time='{{end_time}}')" as cow_per_batch
    inner join cow_amm_surplus on cow_per_batch.tx_hash = cow_amm_surplus.tx_hash
    inner join cow_protocol_gnosis.batches as b on cow_per_batch.tx_hash = b.tx_hash
),

aggregate_results_per_solver_ethereum as (
    select
        name as solver_name,
        sum(realized_cow_surplus_in_usd) as total_cow_surplus_in_usd
    from cow_surplus_per_batch_ethereum
    inner join cow_protocol_ethereum.solvers as s on cow_surplus_per_batch_ethereum.solver_address = s.address and s.active
    group by name
),

aggregate_results_per_solver_gnosis as (
    select
        name as solver_name,
        sum(realized_cow_surplus_in_usd) as total_cow_surplus_in_usd
    from cow_surplus_per_batch_gnosis
    inner join cow_protocol_gnosis.solvers as s on cow_surplus_per_batch_gnosis.solver_address = s.address and s.active
    group by name
),

aggregate_results_per_solver_all_chains_temp as (
    select *
    from aggregate_results_per_solver_ethereum
    union all
    select *
    from aggregate_results_per_solver_gnosis
),

aggregate_results_per_solver_all_chains as (
    select
        solver_name,
        sum(total_cow_surplus_in_usd) as total_cow_surplus_in_usd
    from
        aggregate_results_per_solver_all_chains_temp
    group by solver_name
),

total_surplus as (
    select sum(total_cow_surplus_in_usd) as total_surplus_in_usd from aggregate_results_per_solver_all_chains
),

final_results_per_solver_prelim as (
    select
        arps.solver_name,
        total_cow_surplus_in_usd,
        {{cow_budget}} * arps.total_cow_surplus_in_usd / ts.total_surplus_in_usd as total_cow_reward
    from aggregate_results_per_solver_all_chains as arps cross join total_surplus as ts
),

named_results as (
    select
        reward_target,
        substring(solver_name, 6, 100) as solver_name
    from "query_1541516(end_time='{{end_time}}',vouch_cte_name='named_results')"
),

final_results_per_solver as (
    select distinct  --noqa: ST06
        nr.reward_target,
        frpsp.*
    from final_results_per_solver_prelim as frpsp inner join named_results as nr on frpsp.solver_name = nr.solver_name
    where frpsp.total_cow_reward > 0
)

select * from {{results}}
