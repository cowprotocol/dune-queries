-- This query returns slippage per solver and per transaction over a period of time,
-- evaluated in both usd and the native token of the chain.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--  {{slippage_table_name}} - slippage_per_transaction for aggregated values per transaction;
--    slippage_per_solver for aggregated values per transaction
--
-- The columns of slippage_per_transaction are
-- - block_time: time of settlement transaction
-- - tx_hash: settlement transaction hash
-- - solver_address: address of the solver executing the settlement
-- - slippage_usd: USD value of slippage
-- - slippage_wei: value of slippage in atoms of native token
--
-- The columns of slippage_per_solver are
-- - solver_address: address of the solver executing the settlement
-- - slippage_usd: USD value of slippage
-- - slippage_wei: value of slippage in atoms of native token
--
-- Results of the query are filtered to not include batches from excluded_batches.
-- Batches are also excluded if there is a non-zero imbalance and no value (in native atoms).

with excluded_batches as (
    select tx_hash from query_3490353
),

slippage_per_transaction as (
    select
        rs.block_time,
        rs.tx_hash,
        solver_address,
        sum(slippage_usd) as slippage_usd,
        sum(slippage_wei) as slippage_wei
    from "query_4059683(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}',raw_slippage_table_name='raw_slippage_per_transaction')" as rs
    inner join cow_protocol_{{blockchain}}.batches as b
        on rs.tx_hash = b.tx_hash
    where rs.tx_hash not in (select tx_hash from excluded_batches)
    group by 1, 2, 3
    having bool_and(slippage_wei is not null or slippage_atoms = 0)
),

slippage_per_solver as (
    select
        solver_address,
        sum(slippage_usd) as slippage_usd,
        sum(slippage_wei) as slippage_wei
    from slippage_per_transaction
    group by 1
)

select * from {{slippage_table_name}}
