-- noqa: disable = all
with
auction_range as (
    select
        environment,
        min(auction_id) as min_auction_id,
        max(auction_id) as max_auction_id,
        min(block_deadline) as min_block_deadline,
        max(block_deadline) as max_block_deadline
    from "query_5270914(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
    group by environment
)
, vouch_registry as (
    select * from dune.cowprotocol.result_tmp_vouch_registry_named_results_mainnet
)
, cow_prices as (
    select
        date(minute) as date
        , avg(price) as price
    from prices.usd
    where
        blockchain = 'ethereum' -- use cow price from mainnet
        and contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
        and minute >= timestamp '{{start_time}}'
        and minute < timestamp '{{end_time}}'
    group by 1
)
, native_token_prices as (
    select
        date(minute) as date
        , avg(price) as price
    from prices.usd
    where
        blockchain = '{{blockchain}}'
        and contract_address = (
                    select
                        case
                            when '{{blockchain}}' = 'ethereum' then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
                            when '{{blockchain}}' = 'gnosis' then 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d
                            when '{{blockchain}}' = 'arbitrum' then 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1
                            when '{{blockchain}}' = 'base' then 0x4200000000000000000000000000000000000006
                            when '{{blockchain}}' = 'avalanche_c' then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
                            when '{{blockchain}}' = 'polygon' then 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
                        end
                )
        and minute >= timestamp '{{start_time}}'
        and minute < timestamp '{{end_time}}'
    group by 1
)
, conv_native_to_cow as (
    select n.date, n.price/cow.price as native_to_cow_rate
    from native_token_prices n
    join cow_prices cow
        on n.date=cow.date
)
--------------------------------------------------------------------------------
-- PERFORMANCE REWARDS + NETWORK FEES + EXECUTION COSTS
, auction_data_prep as (
    select
        ad.environment,
        ad.auction_id,
        b.time as block_time,
        ad.solver,
        ad.total_network_fee,
        ad.total_execution_cost,
        ad.capped_payment
    from "query_5270914(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')" as ad
    left join {{blockchain}}.blocks b 
        on ad.block_deadline = b.number
)
, auction_data_in_native as (
    select
        solver,
        block_time,
        coalesce( cast(sum(capped_payment) as double)       / pow(10, 18), 0) as primary_reward_native,
        coalesce( cast(sum(total_network_fee) as double)    / pow(10, 18), 0) as network_fee_native,
        coalesce( cast(sum(total_execution_cost) as double) / pow(10, 18), 0) as execution_cost_native
    from auction_data_prep
    group by 1,2
)
, auction_data as (
    select
        solver,
        block_time,
        primary_reward_native,
        network_fee_native,
        execution_cost_native,
        primary_reward_native * p.native_to_cow_rate as primary_reward_cow,
        network_fee_native    * p.native_to_cow_rate as network_fee_cow,
        execution_cost_native * p.native_to_cow_rate as execution_cost_cow        
    from auction_data_in_native ad
    -- get avg prices on day before payout date
    left join conv_native_to_cow as p 
        on date_trunc('week', ad.block_time - interval '1' day) + interval '7' day = p.date 
)
--------------------------------------------------------------------------------
-- QUOTE REWARDS
, order_quotes as (
    select
        od.order_uid,
        od.quote_solver
    from "query_4364122(blockchain='{{blockchain}}')" as od
    inner join auction_range
        on od.environment = auction_range.environment
        and od.auction_id >= auction_range.min_auction_id 
        and od.auction_id <= auction_range.max_auction_id
)
, winning_quotes as (
    select
        oq.order_uid,
        oq.quote_solver as solver,
        t.block_time        
    from order_quotes as oq
    inner join cow_protocol_{{blockchain}}.trades as t 
        on oq.order_uid = t.order_uid 
        and oq.quote_solver != 0x0000000000000000000000000000000000000000
)
, quote_rewards_in_native as (
    select
        solver,
        block_time,
        least({{quote_reward}}, {{quote_cap_native_token}}) * count(1) as quote_reward_native
    from winning_quotes as wq
    group by 1,2
)
, quote_rewards as (
    select
        solver,
        block_time,
        quote_reward_native,
        quote_reward_native * p.native_to_cow_rate as quote_reward_cow
    from quote_rewards_in_native as qr
    -- get avg prices on day before payout date
    left join conv_native_to_cow as p 
        on date_trunc('week', qr.block_time - interval '1' day) + interval '7' day = p.date 
)
--------------------------------------------------------------------------------
-- SLIPPAGE
, solver_slippage_in_native as (
    select
        solver_address as solver,
        block_time,
        sum(slippage_wei) / pow(10, 18) as slippage_native
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
    -- get avg prices on day before payout date
    left join conv_native_to_cow as p 
        on date_trunc('week', s.block_time - interval '1' day) + interval '7' day = p.date 
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
, service_fee_flag as (
    select
        solver,
        service_fee,
        case when service_fee then 0.85 else 1 end as service_fee_factor
    from "query_4298142(blockchain='{{blockchain}}', start_time='{{start_time}}', end_time='{{end_time}}')"
)
, service_fee_correction as (
    select  --noqa: ST06
        cd.block_time,
        cd.solver,
        cd.name,
        coalesce(sff.service_fee, false) as service_fee_enabled,
        
        case
            when cd.primary_reward_native < 0 then cd.primary_reward_native
            else coalesce(sff.service_fee_factor, 1) * cd.primary_reward_native
        end as primary_reward_native,
        case
            when cd.primary_reward_cow < 0 then cd.primary_reward_cow
            else coalesce(sff.service_fee_factor, 1) * cd.primary_reward_cow
        end as primary_reward_cow,        
        cd.network_fee_native,
        cd.network_fee_cow,
        cd.execution_cost_native,
        cd.execution_cost_cow,

        coalesce(sff.service_fee_factor, 1) * cd.quote_reward_native as quote_reward_native,
        coalesce(sff.service_fee_factor, 1) * cd.quote_reward_cow as quote_reward_cow,
        
        cd.slippage_native,
        cd.slippage_cow      
        
    from combined_data as cd 
    left join service_fee_flag as sff 
        on cd.solver = sff.solver
)
, extended_payout_data as ( -- computed fields used to simplify upcoming case logic
    select 
        *,
        coalesce(primary_reward_native + slippage_native + network_fee_native < 0, false) as is_overdraft,
        primary_reward_native + slippage_native + network_fee_native as total_outgoing_native,
        slippage_native + network_fee_native as reimbursement_native,
        slippage_cow + network_fee_cow as reimbursement_cow
    from service_fee_correction 
) 

select  --noqa: ST06
    block_time,
    (date_trunc('week', block_time - interval '1' day) + interval '8' day) as payout_date, --tuesday
    name,
    p.solver as solver_address,
    vr.reward_target,
    service_fee_enabled,
    
    quote_reward_native,
    quote_reward_cow,
    
    primary_reward_native,
    primary_reward_cow,
    
    slippage_native,
    slippage_cow,
    
    network_fee_native,   
    network_fee_cow,  

    execution_cost_native,     
    execution_cost_cow,
    
    reimbursement_native,
    reimbursement_cow,
        
    case
        when is_overdraft then null
        when reimbursement_native > 0 and primary_reward_cow < 0
            then reimbursement_native + primary_reward_native
        when reimbursement_native < 0 and primary_reward_cow > 0
            then 0
        else reimbursement_native
    end as native_token_transfer,
    
    case
        when is_overdraft then null
        when reimbursement_native > 0 and primary_reward_cow < 0
            then 0
        when reimbursement_native < 0 and primary_reward_cow > 0
            then reimbursement_cow + primary_reward_cow
        else primary_reward_cow
    end as cow_transfer,
    
    case when is_overdraft then total_outgoing_native end as overdraft
    
from extended_payout_data p
left join vouch_registry as vr 
    on p.solver = vr.solver
