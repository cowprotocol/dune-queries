with rewards_per_solver as (
    select
        solver_name,
        sum(protocol_fee / 1e18) as protocol_fee,
        sum(new_protocol_fee / 1e18) as new_protocol_fee,
        sum(reward / 1e18) as reward,
        sum(new_reward_proposal / 1e18) as new_reward_proposal,
        sum(new_reward_fee_cap / 1e18) as new_reward_fee_cap
    from "query_5787562(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}',scaling='{{scaling}}',price_improvement_fee='{{price_improvement_fee}}',volume_fee_bps='{{volume_fee_bps}}',fixed_fee='{{fixed_fee}}')"
    group by 1
)

select * from rewards_per_solver
order by reward desc
