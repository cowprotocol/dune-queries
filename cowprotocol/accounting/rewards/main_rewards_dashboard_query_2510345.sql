with

block_range as (
    select * from "query_3333356(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
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
batch_rewards as (
    select
        rbr.solver as winning_solver,
        rbr.network_fee as fee,
        rbr.execution_cost,
        --rnr.capped_payment,
        case
            when rbr.uncapped_payment_native_token > {{upper_cap}} * pow(10, 18) then {{upper_cap}} * (pow(10, 18))
            when rbr.uncapped_payment_native_token < {{lower_cap}} * pow(10, 18) then {{lower_cap}} * pow(10, 18)
            else rbr.uncapped_payment_native_token
        end as capped_payment
    from "query_4351957(blockchain='{{blockchain}}')" as rbr
    where
        rbr.block_deadline >= (select start_block from block_range)
        and rbr.block_deadline <= (select end_block from block_range)
),

-- AKA Performance Rewards
primary_rewards as (
    select
        winning_solver as solver,
        cast(sum(capped_payment) as double) as reward_wei
    from batch_rewards
    group by winning_solver
),

fees_and_costs as (
    select
        winning_solver as solver,
        cast(sum(fee) as double) as network_fee_wei,
        cast(sum(execution_cost) as double) as execution_cost_wei
    from batch_rewards
    group by winning_solver
),

conversion_prices as (
    select
        (
            select avg(price) from prices.usd
            where
                blockchain = 'ethereum' -- use cow price from mainnet
                and contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
                and date(minute) = cast('{{end_time}}' as timestamp) - interval '1' day
        ) as cow_price,
        (
            select avg(price) from prices.usd
            where
                blockchain = '{{blockchain}}' -- use native prices from respective chains
                and contract_address = (
                    select
                        case
                            when '{{blockchain}}' = 'ethereum' then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
                            when '{{blockchain}}' = 'gnosis' then 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d
                            when '{{blockchain}}' = 'arbitrum' then 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1
                            when '{{blockchain}}' = 'base' then 0x4200000000000000000000000000000000000006
                        end
                )
                and date(minute) = cast('{{end_time}}' as timestamp) - interval '1' day
        ) as native_token_price
),

-- BEGIN QUOTE REWARDS
order_quotes as (
    select
        order_uid,
        quote_solver
    from "query_4364122(blockchain='{{blockchain}}')"
    where
        block_number >= (select start_block from block_range)
        and block_number <= (select end_block from block_range)
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
        coalesce(sff.service_fee_factor, 1) * cd.primary_reward_eth as primary_reward_eth,
        coalesce(sff.service_fee_factor, 1) * cd.primary_reward_cow as primary_reward_cow,
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
