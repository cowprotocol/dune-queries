with

block_range as (
    select * from "query_3333356(start_time='{{start_time}}',end_time='{{end_time}}')"
),

results_per_tx as (
    select * from "query_3427730(start_time='{{start_time}}',end_time='{{end_time}}',cte_name='results_per_tx')"

),

solver_slippage as (
    select
        solver_address as solver,
        sum(eth_slippage_wei) * 1.0 / pow(10, 18) as slippage
    from results_per_tx
    group by solver_address
),

named_results as (
    select * from "query_1541516(end_time='{{end_time}}',vouch_cte_name='named_results')"
),

-- BEGIN SOLVER REWARDS
batch_rewards as (
    select  --noqa: ST06
        rbr.block_deadline,
        rbr.block_number,  -- Null here means the settlement did not occur.
        from_hex(rbr.solver) as winning_solver,
        from_hex(rbr.tx_hash) as tx_hash,
        -- Unpacking the data
        cast(cast(rbr.data.winning_score as varchar) as int256) as winning_score,  -- noqa: RF01
        cast(cast(rbr.data.reference_score as varchar) as int256) as reference_score,  -- noqa: RF01
        cast(cast(rbr.data.surplus as varchar) as int256) as surplus,  -- noqa: RF01
        cast(cast(rbr.data.fee as varchar) as int256) as fee,  -- noqa: RF01
        cast(cast(rbr.data.execution_cost as varchar) as int256) as execution_cost,  -- noqa: RF01
        cast(cast(rbr.data.capped_payment as varchar) as int256) as capped_payment  -- noqa: RF01
    from cowswap.raw_batch_rewards as rbr
    where
        rbr.block_deadline > (select start_block from block_range)
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
                blockchain = 'ethereum'
                and contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
                and date(minute) = cast('{{end_time}}' as timestamp) - interval '1' day
        ) as cow_price,
        (
            select avg(price) from prices.usd
            where
                blockchain = 'ethereum'
                and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
                and date(minute) = cast('{{end_time}}' as timestamp) - interval '1' day
        ) as eth_price
),

-- BEGIN QUOTE REWARDS
order_quotes as (
    select
        order_uid,
        quote_solver
    from cow_protocol_ethereum.order_rewards
    where
        block_number > (select start_block from block_range)
        and block_number <= (select end_block from block_range)
),

winning_quotes as (
    select
        oq.order_uid,
        quote_solver as solver
    from order_quotes as oq
    inner join cow_protocol_ethereum.trades as t on oq.order_uid = t.order_uid and oq.quote_solver != 0x0000000000000000000000000000000000000000
),

quote_rewards as (
    select
        solver,
        least({{quote_reward}}, {{quote_cap_eth}} * (select eth_price / cow_price from conversion_prices)) * count(*) as quote_reward
    from winning_quotes group by solver
),

aggregate_results as (
    select
        pr.solver,
        coalesce(reward_wei, 0) / pow(10, 18) as primary_reward_eth,
        coalesce(network_fee_wei, 0) / pow(10, 18) as network_fee_eth,
        coalesce(execution_cost_wei, 0) / pow(10, 18) as execution_cost_eth,
        coalesce(reward_wei, 0) / pow(10, 18) * (select eth_price / cow_price from conversion_prices) as primary_reward_cow
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
            '&start_time={{start_time}}&end_time={{end_time}}&min_absolute_slippage_tolerance=0&relative_slippage_tolerance=0&significant_slippage_value=0" target="_blank">link</a>'
        ) as slippage_per_tx,
        concat(environment, '-', name) as name  --noqa: RF04
    from aggregate_results as ar
    full outer join solver_slippage as ss
        on ar.solver = ss.solver
    full outer join quote_rewards as qr
        on ar.solver = qr.solver
    left join cow_protocol_ethereum.solvers as s
        on coalesce(ar.solver, ss.solver, qr.solver) = s.address
),

service_fee_flag as (
    select
        solver,
        service_fee,
        case
            when service_fee is true then 0.85
            else 1
        end as service_fee_factor
    from "query_4017925"
),

combined_data_after_service_fee as (
    select  --noqa: ST06
        cd.solver,
        cd.network_fee_eth,
        cd.execution_cost_eth,
        sff.service_fee_factor * cd.primary_reward_eth as primary_reward_eth,
        sff.service_fee_factor * cd.primary_reward_cow as primary_reward_cow,
        sff.service_fee_factor * cd.quote_reward as quote_reward,
        cd.slippage_eth,
        cd.slippage_per_tx,
        cd.name,
        sff.service_fee as service_fee_enabled
    from combined_data as cd inner join service_fee_flag as sff on cd.solver = sff.solver
),

extended_payout_data as (
    select --noqa: ST06
        cd.*,
        -- computed fields used to simplify case logic.
        cd.primary_reward_eth + cd.slippage_eth + cd.network_fee_eth as total_outgoing_eth,
        coalesce(cd.primary_reward_eth + cd.slippage_eth + cd.network_fee_eth < 0, false) as is_overdraft,
        cd.slippage_eth + cd.network_fee_eth as reimbursement_eth,
        (cd.slippage_eth + cd.network_fee_eth) * (select eth_price / cow_price from conversion_prices) as reimbursement_cow,
        cd.primary_reward_cow as total_cow_reward,
        cd.primary_reward_eth as total_eth_reward
    from combined_data_after_service_fee as cd
),

final_results as (
    select  --noqa: ST06
        epd.*,
        case
            when is_overdraft then null
            when reimbursement_eth > 0 and total_cow_reward < 0
                then reimbursement_eth + total_eth_reward
            when reimbursement_eth < 0 and total_cow_reward > 0
                then 0
            else reimbursement_eth
        end as eth_transfer,
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
        reward_target
    from extended_payout_data as epd
    left join named_results as nr on epd.solver = nr.solver
)

select * from final_results
