with rewards_total as (
    select
        sum(protocol_fee / 1e18) as protocol_fee,
        sum(new_protocol_fee / 1e18) as new_protocol_fee,
        sum(reward / 1e18) as reward,
        sum(new_reward_fee_cap / 1e18) as new_reward_fee_cap,
        sum(protocol_fee / 1e18) - sum(reward / 1e18) as profit,
        sum(new_protocol_fee / 1e18) - sum(new_reward_fee_cap / 1e18) as new_profit, 
        sum(greatest(uncapped_reward, 0) / 1e18 - reward / 1e18) as truthful_bidding_rewards_missed,
        sum(greatest(uncapped_reward, 0) / 1e18 - new_reward_fee_cap / 1e18) as new_truthful_bidding_rewards_missed,
        100.00 * count_if(uncapped_reward > reward) / count(*) as untruthful_auctions_ratio,
        100.00 * count_if(uncapped_reward > new_reward_fee_cap) / count(*) as new_untruthful_auctions_ratio
    from "query_5787562(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}',scaling='{{scaling}}',price_improvement_fee='{{price_improvement_fee}}',volume_fee_bps='{{volume_fee_bps}}',fixed_fee='{{fixed_fee}}')"
)

select * from rewards_total
