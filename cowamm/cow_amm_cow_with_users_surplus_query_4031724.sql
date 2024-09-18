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

bonus_per_batch as (
    select
        cow_per_batch.block_time,
        cow_per_batch.tx_hash,
        solver_address,
        naive_cow_potential,
        naive_cow,
        surplus,
        naive_cow_potential * surplus as bonus_reward_potential,
        naive_cow * surplus as bonus_reward
    from "query_4025739(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')" as cow_per_batch
    inner join cow_amm_surplus on cow_per_batch.tx_hash = cow_amm_surplus.tx_hash
    inner join cow_protocol_{{blockchain}}.batches as b on cow_per_batch.tx_hash = b.tx_hash
),

aggregate_results_per_solver as (
    select
        name as solver_name,
        sum(bonus_reward_potential) as bonus_reward_potential,
        sum(bonus_reward) as bonus_reward
    from bonus_per_batch
    inner join cow_protocol_{{blockchain}}.solvers as s on bonus_per_batch.solver_address = s.address and s.active
    group by name
),

total_surplus as (
    select sum(bonus_reward) as total_surplus from aggregate_results_per_solver
),

bounty_distribution as (
    select
        arps.solver_name,
        {{budget}} * arps.bonus_reward / ts.total_surplus as total_cow_reward
    from aggregate_results_per_solver as arps cross join total_surplus as ts
)

select * from {{results}}
