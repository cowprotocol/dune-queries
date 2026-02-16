-- This query returns a detailed breakdown on the different types of reward or reimbursement solvers get, on a auction basis
-- The timestamp associated to each auction is the block deadline's one

-- params:
-- start and end_time: period to consider (usually Tuesday 00:00 UTC to Tuesday 00:00 UTC to consider the payout cycle)
-- blockchain: specific chain 

-- columns:
-- quote rewards = reward for offering the winning quote
-- primary rewards = performance reward / reward for solving
-- network fee = amounts the solvers should be reimbursed due to their gas spend
-- slippage = imbalance generated in the settlement contract during a given auction 
-- execution costs = actual solver's gas cost of a given batch

--noqa: disable=all
with
auction_range as (
    select
        environment,
        min(auction_id) as min_auction_id,
        max(auction_id) as max_auction_id
    from "query_5270914(blockchain='{{blockchain}}', start_time='{{start_time}}', end_time='{{end_time}}')"
    group by environment
)
, vouch_registry as (
    select * from "query_1541516(blockchain='{{blockchain}}', end_time='{{end_time}}', vouch_cte_name='named_results')"
)
, quote_cap_mapping as (
    select *
    from 
        (values
            -- Pre CIP 72
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.0006, 6, 'ethereum'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.15,   6, 'gnosis'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.0002, 6, 'base'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.0002, 6, 'arbitrum'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.0005, 6, 'avalanche_c'),
            (timestamp '2010-01-01 00:00', timestamp '2025-08-12 00:00', 0.5,    6, 'polygon'),
            -- Post CIP 72
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.0007,  6, 'ethereum'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.15,    6, 'gnosis'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.00024, 6, 'base'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.00024, 6, 'arbitrum'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.0006,  6, 'avalanche_c'),
            (timestamp '2025-08-12 00:00', timestamp '2099-01-01 00:00', 0.6,     6, 'polygon')
    
        ) as t(from_ts, until_ts, quote_cap_native, quote_cap_cow, blockchain)
)
, conv_native_to_cow as (
    select
        end_time as date,
        native_token_price / nullif(cow_price, 0) as native_to_cow_rate
    from dune.cowprotocol.result_accounting_cow_and_native_prices_per_chain
    where
        blockchain = '{{blockchain}}'
        and end_time > date_add('day', -1, cast('{{end_time}}' as timestamp))
        and end_time < date_add('day', +1, cast('{{end_time}}' as timestamp))
)
--------------------------------------------------------------------------------
-- PERFORMANCE REWARDS + NETWORK FEES + EXECUTION COSTS
, auction_data_prep as (
    select
        ad.environment,
        ad.auction_id,
        ad.block_deadline,
        b.time as block_time,
        ad.solver,
        ad.total_network_fee,
        ad.total_execution_cost,
        ad.capped_payment
    from "query_5270914(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')" as ad
    inner join auction_range as ar
        on ad.environment = ar.environment
        and ad.auction_id between ar.min_auction_id and ar.max_auction_id
    left join {{blockchain}}.blocks b 
        on ad.block_deadline = b.number
)
, auction_data_in_native as (
    select
        solver,
        auction_id,
        block_deadline,
        block_time,
        coalesce( cast(sum(capped_payment) as double), 0)       / pow(10, 18) as primary_reward_native,
        coalesce( cast(sum(total_network_fee) as double), 0)    / pow(10, 18) as network_fee_native,
        coalesce( cast(sum(total_execution_cost) as double), 0) / pow(10, 18) as execution_cost_native
    from auction_data_prep
    group by 1,2,3,4
)
, auction_data as (
    select
        solver,
        auction_id,
        block_deadline,
        block_time,
        
        primary_reward_native,
        network_fee_native,
        execution_cost_native,
        
        primary_reward_native * p.native_to_cow_rate as primary_reward_cow,
        network_fee_native    * p.native_to_cow_rate as network_fee_cow,
        execution_cost_native * p.native_to_cow_rate as execution_cost_cow        
    from auction_data_in_native ad 
    left join conv_native_to_cow as p 
        on date_trunc('week', ad.block_time - interval '1' day) + interval '8' day = p.date 
) 
--------------------------------------------------------------------------------
-- QUOTE REWARDS
, winning_quotes as (
    select
        od.auction_id,
        t.block_time,
        od.quote_solver as solver
    from "query_4364122(blockchain='{{blockchain}}')" as od
    inner join auction_range as ar 
        on od.environment = ar.environment
        and od.auction_id between ar.min_auction_id and ar.max_auction_id      
    inner join cow_protocol_{{blockchain}}.trades as t 
        on od.order_uid = t.order_uid 
        and od.quote_solver != 0x0000000000000000000000000000000000000000
) 
, quote_rewards as (
    select
        wq.solver,
        wq.auction_id,
        wq.block_time,
        least(cap.quote_cap_cow, cap.quote_cap_native * p.native_to_cow_rate)  as quote_reward_cow,
        least(cap.quote_cap_cow, cap.quote_cap_native * p.native_to_cow_rate) / p.native_to_cow_rate  as quote_reward_native
    from winning_quotes as wq
    left join quote_cap_mapping as cap
        on wq.block_time > cap.from_ts
        and wq.block_time <= cap.until_ts
        and cap.blockchain = '{{blockchain}}'
    left join conv_native_to_cow as p 
        on date_trunc('week', wq.block_time - interval '1' day) + interval '8' day = p.date
)
--------------------------------------------------------------------------------
-- SLIPPAGE
, solver_slippage_in_native as (
    select
        solver_address as solver,
        block_time,
        cast(sum(slippage_wei) as double) / pow(10, 18) as slippage_native
    from "query_4070065(blockchain='{{blockchain}}', start_time='{{start_time}}', end_time='{{end_time}}', slippage_table_name='slippage_per_transaction')" s
    group by 1,2
)
, solver_slippage as (
    select
        solver,
        block_time,
        slippage_native,
        slippage_native * p.native_to_cow_rate as slippage_cow
    from solver_slippage_in_native s
    left join conv_native_to_cow as p 
        on date_trunc('week', s.block_time - interval '1' day) + interval '8' day = p.date 
)
--------------------------------------------------------------------------------
, combined_data as (
    select
        coalesce(ad.block_time, sl.block_time, qr.block_time) as block_time,
        coalesce(ad.solver, sl.solver, qr.solver) as solver,
        concat(s.environment, '-', s.name) as name,        
        
        ad.primary_reward_native,
        ad.network_fee_native,
        ad.execution_cost_native,        
        ad.primary_reward_cow,
        ad.network_fee_cow,
        ad.execution_cost_cow,
        
        coalesce(qr.quote_reward_native, 0) as quote_reward_native,
        coalesce(qr.quote_reward_cow, 0) as quote_reward_cow,
        
        coalesce(sl.slippage_native, 0) as slippage_native,
        coalesce(sl.slippage_cow, 0) as slippage_cow
    
    from auction_data as ad
    full outer join quote_rewards as qr
        on ad.solver = qr.solver
        and ad.block_time = qr.block_time
    full outer join solver_slippage as sl
        on ad.solver = sl.solver
        and ad.block_time = sl.block_time
    left join cow_protocol_{{blockchain}}.solvers as s
        on coalesce(ad.solver, sl.solver, qr.solver) = s.address
)
select  --noqa: ST06
    block_time,
    (date_trunc('week', block_time - interval '1' day) + interval '8' day) as payout_date, --tuesday
    name as solver,
    cd.solver as solver_address,
    vr.reward_target as reward_address,
    
    quote_reward_native,
    quote_reward_cow,
    
    primary_reward_native,
    primary_reward_cow,
    
    network_fee_native,   
    network_fee_cow,  

    slippage_native,
    slippage_cow,
    
    execution_cost_native,     
    execution_cost_cow

from combined_data as cd
left join vouch_registry as vr 
    on cd.solver = vr.solver
