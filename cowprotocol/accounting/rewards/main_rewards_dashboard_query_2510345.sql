with

auction_range as (
    select
        environment,
        min(auction_id) as min_auction_id,
        max(auction_id) as max_auction_id
    from "query_5270914(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
    group by environment
),

solver_slippage as (
    select
        solver_address as solver,
        slippage_wei * 1.0 / pow(10, 18) as slippage
    from "query_4070065(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}',slippage_table_name='slippage_per_solver')"
),

named_results as (
    select * from "query_1541516(blockchain='{{blockchain}}',end_time='{{end_time}}',vouch_cte_name='named_results')"
),

-- BEGIN SOLVER REWARDS
auction_data as (
    select
        ad.environment,
        ad.auction_id,
        ad.solver,
        ad.total_network_fee,
        ad.total_execution_cost,
        ad.capped_payment
    from "query_5270914(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')" as ad
    inner join auction_range on ad.environment = auction_range.environment
    where ad.auction_id >= auction_range.min_auction_id and ad.auction_id <= auction_range.max_auction_id
),

auction_data_filtered as (
    select
        ad.environment,
        ad.auction_id,
        ad.solver,
        ad.total_network_fee,
        ad.total_execution_cost,
        ad.capped_payment * coalesce(ea.multiplier, 1) as capped_payment
    from auction_data as ad left outer join "query_4842868(blockchain='{{blockchain}}')" as ea on ad.environment = ea.environment and ad.auction_id = ea.auction_id
),

-- AKA Performance Rewards
primary_rewards as (
    select
        solver,
        cast(sum(capped_payment) as double) as reward_wei
    from auction_data_filtered
    group by solver
),

fees_and_costs as (
    select
        solver,
        cast(sum(total_network_fee) as double) as network_fee_wei,
        cast(sum(total_execution_cost) as double) as execution_cost_wei
    from auction_data_filtered
    group by solver
),

conversion_prices as (
    select
        cow_price,
        native_token_price
    from dune.cowprotocol.result_accounting_cow_and_native_prices_per_chain
    where
        blockchain = '{{blockchain}}'
        and end_time > date_add('day', -1, cast('{{end_time}}' as timestamp))
        and end_time < date_add('day', +1, cast('{{end_time}}' as timestamp))
),

-- BEGIN QUOTE REWARDS
order_quotes as (
    select
        od.order_uid,
        od.quote_solver
    from "query_4364122(blockchain='{{blockchain}}')" as od
    inner join auction_range on od.environment = auction_range.environment
    where od.auction_id >= auction_range.min_auction_id and od.auction_id <= auction_range.max_auction_id
),

winning_quotes as (
    select
        oq.order_uid,
        quote_solver as solver
    from order_quotes as oq
    inner join cow_protocol_{{blockchain}}.trades as t on oq.order_uid = t.order_uid and oq.quote_solver != 0x0000000000000000000000000000000000000000
),

quote_rewards as (
    select
        solver,
        least({{quote_reward}}, {{quote_cap_native_token}} * (select native_token_price / cow_price from conversion_prices)) * count(*) as quote_reward
    from winning_quotes group by solver
),

aggregate_results as (
    select
        pr.solver,
        coalesce(reward_wei, 0) / pow(10, 18) as primary_reward_eth,
        coalesce(network_fee_wei, 0) / pow(10, 18) as network_fee_eth,
        coalesce(execution_cost_wei, 0) / pow(10, 18) as execution_cost_eth,
        coalesce(reward_wei, 0) / pow(10, 18) * (select native_token_price / cow_price from conversion_prices) as primary_reward_cow
    from primary_rewards as pr left outer join fees_and_costs as fc on pr.solver = fc.solver
),

combined_data as (
    select
        coalesce(ar.solver, ss.solver, qr.solver) as solver,
        network_fee_eth,
        execution_cost_eth,
        primary_reward_eth,
        primary_reward_cow,
        coalesce(quote_reward, 0) as quote_reward,
        coalesce(slippage, 0) as slippage_eth,
        concat(
            '<a href="https://dune.com/queries/2332678?SolverAddress=',
            cast(ar.solver as varchar),
            '&blockchain={{blockchain}}&start_time={{start_time}}&end_time={{end_time}}&min_absolute_slippage_tolerance=0&relative_slippage_tolerance=0&significant_slippage_value=0" target="_blank">link</a>'
        ) as slippage_per_tx,
        concat(environment, '-', name) as name  --noqa: RF04
    from aggregate_results as ar
    full outer join solver_slippage as ss
        on ar.solver = ss.solver
    full outer join quote_rewards as qr
        on ar.solver = qr.solver
    left join cow_protocol_{{blockchain}}.solvers as s
        on coalesce(ar.solver, ss.solver, qr.solver) = s.address
),

service_fee_flag as (
    select
        solver,
        service_fee,
        case
            when service_fee then 0.85
            else 1
        end as service_fee_factor
    from "query_4298142(blockchain='{{blockchain}}', start_time='{{start_time}}', end_time='{{end_time}}')"
),

combined_data_after_service_fee as (
    select  --noqa: ST06
        cd.solver,
        cd.network_fee_eth,
        cd.execution_cost_eth,
        case
            when cd.primary_reward_eth < 0 then cd.primary_reward_eth
            else coalesce(sff.service_fee_factor, 1) * cd.primary_reward_eth
        end as primary_reward_eth,
        case
            when cd.primary_reward_cow < 0 then cd.primary_reward_cow
            else coalesce(sff.service_fee_factor, 1) * cd.primary_reward_cow
        end as primary_reward_cow,
        coalesce(sff.service_fee_factor, 1) * cd.quote_reward as quote_reward,
        cd.slippage_eth,
        cd.slippage_per_tx,
        cd.name,
        coalesce(sff.service_fee, false) as service_fee_enabled
    from combined_data as cd left outer join service_fee_flag as sff on cd.solver = sff.solver
),

extended_payout_data as (
    select --noqa: ST06
        cd.*,
        -- computed fields used to simplify case logic.
        cd.primary_reward_eth + cd.slippage_eth + cd.network_fee_eth as total_outgoing_eth,
        coalesce(cd.primary_reward_eth + cd.slippage_eth + cd.network_fee_eth < 0, false) as is_overdraft,
        cd.slippage_eth + cd.network_fee_eth as reimbursement_eth,
        (cd.slippage_eth + cd.network_fee_eth) * (select native_token_price / cow_price from conversion_prices) as reimbursement_cow,
        cd.primary_reward_cow as total_cow_reward,
        cd.primary_reward_eth as total_eth_reward
    from combined_data_after_service_fee as cd
)

select  --noqa: ST06
    name,
    epd.solver as solver_address,
    reward_target,
    quote_reward,
    case
        when is_overdraft then null
        when reimbursement_eth > 0 and total_cow_reward < 0
            then reimbursement_eth + total_eth_reward
        when reimbursement_eth < 0 and total_cow_reward > 0
            then 0
        else reimbursement_eth
    end as native_token_transfer,
    case
        when is_overdraft then null
        when reimbursement_eth > 0 and total_cow_reward < 0
            then 0
        when reimbursement_eth < 0 and total_cow_reward > 0
            then reimbursement_cow + total_cow_reward
        else total_cow_reward
    end as cow_transfer,
    case
        when is_overdraft then total_outgoing_eth
    end as overdraft,
    slippage_eth as slippage_native_token,
    slippage_per_tx,
    service_fee_enabled,
    reimbursement_eth as reimbursement_native_token,
    reimbursement_cow,
    total_cow_reward,
    network_fee_eth as network_fee_native_token,
    execution_cost_eth as execution_cost_native_token
from extended_payout_data as epd
left join named_results as nr on epd.solver = nr.solver
