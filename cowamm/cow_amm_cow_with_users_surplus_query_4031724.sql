-- This query computes how much surplus has been provided to CoW AMMs, when trading with other user orders
-- as part of a CoW. For that, a CoW detector query is used (4025739(). Finally, the query computes the 
-- distribution of an amount {{cow_budget}} of COW tokens to solvers, proportionally to the surplus generated
-- via CoWs and pushed to CoW AMMs.
-- Parameters:
--  {{start_time}} - the start date timestamp for the accounting period  (inclusively)
--  {{end_time}} - the end date timestamp for the accounting period (exclusively)
-- {{cow_budget}} -- the amount of COW that needs to be distributed

with cow_surplus_per_batch_ethereum as (
    select
        'ethereum' as blockchain,
        cow_per_batch.block_time,
        cow_per_batch.tx_hash,
        solvers.name as solver_name,
        naive_cow,  -- fraction of batch volume traded within a CoW
        trades.surplus_usd as surplus_in_usd,  -- surplus of the executed CoW AMM order, expressed in USD
        naive_cow * trades.surplus_usd as realized_cow_surplus_in_usd -- surplus of the CoW AMM that is assumed to be generated via a CoW.
    from "query_4025739(blockchain='ethereum',start_time='{{start_time}}',end_time='{{end_time}}')" as cow_per_batch
    inner join cow_protocol_ethereum.trades as trades on cow_per_batch.tx_hash = trades.tx_hash
    inner join cow_protocol_ethereum.batches as batches on cow_per_batch.tx_hash = batches.tx_hash
    inner join cow_protocol_ethereum.solvers as solvers on batches.solver_address = solvers.address and solvers.active
    where trades.trader in (select address from query_3959044 where blockchain = 'ethereum')
),

cow_surplus_per_batch_gnosis as (
    select
        'gnosis' as blockchain,
        cow_per_batch.block_time,
        cow_per_batch.tx_hash,
        solvers.name as solver_name,
        naive_cow,  -- fraction of batch volume traded within a CoW
        trades.surplus_usd as surplus_in_usd,  -- surplus of the executed CoW AMM order, expressed in USD
        naive_cow * trades.surplus_usd as realized_cow_surplus_in_usd -- surplus of the CoW AMM that is assumed to be generated via a CoW.
    from "query_4025739(blockchain='gnosis',start_time='{{start_time}}',end_time='{{end_time}}')" as cow_per_batch
    inner join cow_protocol_gnosis.trades as trades on cow_per_batch.tx_hash = trades.tx_hash
    inner join cow_protocol_gnosis.batches as batches on cow_per_batch.tx_hash = batches.tx_hash
    inner join cow_protocol_gnosis.solvers as solvers on batches.solver_address = solvers.address and solvers.active
    where trades.trader in (select address from query_3959044 where blockchain = 'gnosis')
),

cow_surplus_per_batch_arbitrum as (
    select
        'arbitrum' as blockchain,
        cow_per_batch.block_time,
        cow_per_batch.tx_hash,
        solvers.name as solver_name,
        naive_cow,  -- fraction of batch volume traded within a CoW
        trades.surplus_usd as surplus_in_usd,  -- surplus of the executed CoW AMM order, expressed in USD
        naive_cow * trades.surplus_usd as realized_cow_surplus_in_usd -- surplus of the CoW AMM that is assumed to be generated via a CoW.
    from "query_4025739(blockchain='arbitrum',start_time='{{start_time}}',end_time='{{end_time}}')" as cow_per_batch
    inner join cow_protocol_arbitrum.trades as trades on cow_per_batch.tx_hash = trades.tx_hash
    inner join cow_protocol_arbitrum.batches as batches on cow_per_batch.tx_hash = batches.tx_hash
    inner join cow_protocol_arbitrum.solvers as solvers on batches.solver_address = solvers.address and solvers.active
    where trades.trader in (select address from query_3959044 where blockchain = 'arbitrum')
),

cow_surplus_per_batch as (
    select * from cow_surplus_per_batch_ethereum
    union all
    select * from cow_surplus_per_batch_gnosis
    union all
    select * from cow_surplus_per_batch_arbitrum
),


aggregate_result_per_solver as (
    select
        solver_name,
        sum(realized_cow_surplus_in_usd) as total_cow_surplus_in_usd
    from cow_surplus_per_batch
    group by solver_name
),


---- final results

reward_addresses as (
    select
        solver as solver_address,
        reward_target,
        substring(solver_name, 6, 100) as solver_name
    from "query_1541516(end_time='{{end_time}}',vouch_cte_name='named_results')"
),

final_results_per_solver as (
    select distinct
        a.solver_name,
        b.reward_target,
        a.total_cow_surplus_in_usd,
        {{cow_budget}} * a.total_cow_surplus_in_usd / (select sum(total_cow_surplus_in_usd) from aggregate_result_per_solver) as total_cow_reward
    from aggregate_result_per_solver as a
    inner join reward_addresses as b on a.solver_name = b.solver_name
    where a.total_cow_surplus_in_usd > 0
)

select * from {{results}}
