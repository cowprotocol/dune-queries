with auction_range as (
    select
        environment
        , min(auction_id) as min_auction_id
        , max(auction_id) as max_auction_id
        from "query_5270914(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
        group by
    environment
)
        , solver_slippage as (
        select
        solver_address as solver
        , block_date
        , slippage_wei * 1.0 / pow(10, 18) as slippage
        from "query_6157609(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}',slippage_table_name='slippage_per_solver')"
)
        -- BEGIN SOLVER REWARDS
    , auction_data as (
    , select
        ad.solver
        , ad.block_date
        , ad.total_network_fee
        , ad.capped_payment
        from "query_6157692(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')" as ad
            inner join auction_range on ad.environment = auction_range.environment
        where ad.auction_id >= auction_range.min_auction_id
   and ad.auction_id <= auction_range.max_auction_id
)
        -- AKA Performance Rewards
    , primary_rewards as (
    , select
        solver
        , block_date
        , cast(sum(capped_payment) as double) as reward_wei
        from auction_data
        group by
    solver
    , block_date
)
        , fees_and_costs as (
        select
        solver
        , block_date
        , cast(sum(total_network_fee) as double) as network_fee_wei
        from auction_data
        group by
    solver
    , block_date
)
        , aggregate_results as (
        select
        pr.solver
        , pr.block_date
        , coalesce(reward_wei, 0) / pow(10, 18) as primary_reward_eth
        , coalesce(network_fee_wei, 0) / pow(10, 18) as network_fee_eth
        from primary_rewards as pr
            left outer join fees_and_costs as fc on pr.solver = fc.solver
   and pr.block_date = fc.block_date
)
        , combined_data as (
        select
        coalesce(ar.solver, ss.solver) as solver
        , ar.block_date
        , network_fee_eth
        , primary_reward_eth
        , coalesce(slippage, 0) as slippage_eth
        from aggregate_results as ar
            full outer join solver_slippage as ss on ar.solver = ss.solver
   and ar.block_date = ss.block_date
)
        , service_fee_flag as (
        select
        solver
        , case
when service_fee
    then 0.85
else
    1
end as service_fee_factor
        from "query_4298142(blockchain='{{blockchain}}', start_time='{{start_time}}', end_time='{{end_time}}')"
)
        , combined_data_after_service_fee as (
select --noqa: ST06 cd.solver
        , cd.block_date
        , cd.network_fee_eth
        , case
when cd.primary_reward_eth < 0
    then cd.primary_reward_eth
else
    coalesce(sff.service_fee_factor, 1) * cd.primary_reward_eth
end as primary_reward_eth
        , cd.slippage_eth
        from combined_data as cd
            left outer join service_fee_flag as sff on cd.solver = sff.solver
)
        , daily_solver_rewards as (
select --noqa: ST06 block_date
        , sum(slippage_eth) as slippage_native_token
        , sum(primary_reward_eth) as total_reward_native_token
        , sum(network_fee_eth) as network_fee_native_token
        from combined_data_after_service_fee as epd
        group by
    block_date
)
        , daily_protocol_fee_native as (
        select
        b."date" as block_date
        , sum(
                protocol_fee * protocol_fee_native_price / pow(10, 18) - coalesce(
                    case
when partner_fee_recipient is not null
    then partner_fee * protocol_fee_native_price / pow(10, 18)
end
        , 0
)
) as protocol_fee_in_native_token --noqa: RF01
    , from "query_4364122(blockchain='{{blockchain}}')" as r
            inner join {{blockchain}}.blocks as b on number = block_number
        where b.time between timestamp '{{start_time}}' 
   and timestamp  '{{end_time}}'
   and r.order_uid not in (
                select
                    order_uid
                from query_3639473
)
        group by
    b."date"
)
        , partner_fee as (
        select
        block_date
        , sum(partner_fee_part) as partner_fee_part
        , sum(cow_dao_partner_fee_part) as cow_dao_partner_fee_part
        from "query_6157807(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
        group by
    block_date
)
select
    coalesce(rewards.block_date, fee.block_date, partner_fee.block_date) block_date
    , rewards.slippage_native_token
    , rewards.total_reward_native_token
    , rewards.network_fee_native_token
    , fee.protocol_fee_in_native_token
    , partner_fee.partner_fee_part
    , partner_fee.cow_dao_partner_fee_part
from daily_solver_rewards rewards
    full outer join daily_protocol_fee_native fee on rewards.block_date = fee.block_date
    full outer join partner_fee on rewards.block_date = partner_fee.block_date
order by
    block_date desc