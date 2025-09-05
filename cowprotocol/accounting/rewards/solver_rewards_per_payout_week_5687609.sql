-- This query returns a detailed breakdown on the different types of reward or reimbursement solvers get, on a block_time basis

-- params:
-- quote_cap_native_token and quote_reward: chain-specific limits for rewards on a batch basis, more details in https://docs.cow.fi/cow-protocol/reference/core/auctions/rewards#price-estimation-competition-rewards-cips-27-57
-- start and end_time: period to consider (usually Tuesday 00:00 UTC to Tuesday 00:00 UTC to consider the payout cycle)

-- columns:
-- quote rewards = reward for offering the winning quote
-- primary rewards = performance reward / reward for solving
-- network fee = amounts the solvers should be reimbursed due to their gas spend
-- execution costs = actual gas cost of that batch for the solver 
-- slippage = imbalance generated in the settlement contract during a given auction
-- reimbursement = slippage + network fees
-- overdraft = when the sum of primary_reward + slippage + network_fee is negative
-- native_token_transfer = amount due to the solver in native token, depends on specific logic that can be seen in the end of this code 
-- cow_transfer = amount due to the solver in COW token, depends on specific logic that can be seen in the end of this code (does not including quote rewards)

--noqa: disable=all

with
week_agg as (
    select 
        payout_date 
        , solver
        , solver_address
        , reward_address

        , sum(quote_reward_native) as quote_reward_native
        , sum(quote_reward_cow) as quote_reward_cow
    
        , sum(primary_reward_native) as primary_reward_native
        , sum(primary_reward_cow) as primary_reward_cow
        
        , sum(slippage_native) as slippage_native
        , sum(slippage_cow) as slippage_cow
        
        , sum(network_fee_native) as network_fee_native
        , sum(network_fee_cow) as network_fee_cow
        
        , sum(execution_cost_native) as execution_cost_native
        , sum(execution_cost_cow) as execution_cost_cow
        
    from "query_5703026(blockchain='ethereum', start_time='{{start_time}}', end_time='{{end_time}}')" 
    group by 1,2,3,4
)
, service_fee_flag as (
    select
        solver as solver_address,
        service_fee,
        case when service_fee then 0.85 else 1 end as service_fee_factor
    from "query_4298142(blockchain='{{blockchain}}', start_time='{{start_time}}', end_time='{{end_time}}')"
)
, service_fee_correction as (
    select  
        payout_date 
        , solver
        , wa.solver_address    
        , reward_address  
        , coalesce(sff.service_fee, false) as service_fee_enabled

        , coalesce(sff.service_fee_factor, 1) * quote_reward_native as quote_reward_native
        , coalesce(sff.service_fee_factor, 1) * quote_reward_cow as quote_reward_cow
        
        , case
            when primary_reward_native < 0 then primary_reward_native
            else coalesce(sff.service_fee_factor, 1) * primary_reward_native
        end as primary_reward_native
        , case
            when primary_reward_cow < 0 then primary_reward_cow
            else coalesce(sff.service_fee_factor, 1) * primary_reward_cow
        end as primary_reward_cow
        
        , slippage_native
        , slippage_cow

        , network_fee_native
        , network_fee_cow

        , execution_cost_native
        , execution_cost_cow
        
    from week_agg as wa
    left join service_fee_flag as sff 
        on wa.solver_address = sff.solver_address
)
, payout_logic_prep as (
    select * 
        , slippage_native + network_fee_native as reimbursement_native
        , slippage_cow    + network_fee_cow    as reimbursement_cow
        , coalesce(primary_reward_native + slippage_native + network_fee_native < 0, false) as is_overdraft
    from week_agg
)
select 
    *,
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
    
    if(is_overdraft, primary_reward_native + slippage_native + network_fee_native) as overdraft_native

from payout_logic_prep
order by 1,2
