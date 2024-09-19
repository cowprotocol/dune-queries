-- This query returns slippage per transaction, evaluated in both usd and the native token of the chain.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- The columns of the result are
-- - block_time: time of settlement transaction
-- - tx_hash: settlement transaction hash
-- - solver_address: address of the solver executing the settlement
-- - slippage_usd: USD value of slippage
-- - slippage_wei: value of slippage in native token
--
-- Results of the query are filtered to not include batches from excluded_batches.
-- Batches are also excluded if there is a non-zero imbalance and no value (in native atoms).

with excluded_batches as (
    select tx_hash from query_3490353
)

select
    s.block_time,
    s.tx_hash,
    solver_address,
    sum(slippage_usd) as slippage_usd,
    sum(slippage_native_atom) as slippage_wei
from "query_4059683(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')" as s
inner join cow_protocol_{{blockchain}}.batches as b
    on s.tx_hash = b.tx_hash
where s.tx_hash not in (select tx_hash from excluded_batches)
group by 1, 2, 3
having bool_and(slippage_wei is not null or slippage_atoms = 0)
