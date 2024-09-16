-- This query return slippage per transaction
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
-- - slippage_native: value of slippage in native token

select
    solver_address,
    sum(slippage_usd) as slippage_usd,
    sum(slippage_native) as slippage_native
from "query_4070059(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
group by solver_address
