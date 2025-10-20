with rewards_per_solver as (
    select
        solver_name,
        sum(protocol_fee / 1e18) as protocol_fee,
        sum(new_protocol_fee / 1e18) as new_protocol_fee,
        sum(reward / 1e18) as reward,
        sum(new_reward / 1e18) as new_reward,
        sum(profit / 1e18) as profit,
        sum(new_profit / 1e18) as new_profit, 
        sum(reward_missed / 1e18) as truthful_bidding_rewards_missed,
        sum(new_reward_missed / 1e18) as new_truthful_bidding_rewards_missed,
        count_if(uncapped_reward > reward) as untruthful_auctions,
        count_if(uncapped_reward > new_reward) as new_untruthful_auctions,
        count(*) as auctions,
        sum(volume / 1e18) as volume
    from "query_5787562(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}',scaling='{{scaling}}',price_improvement_fee='{{price_improvement_fee}}',volume_fee_bps='{{volume_fee_bps}}',fixed_fee='{{fixed_fee}}')"
    group by 1
)

select * from rewards_per_solver
order by reward desc
