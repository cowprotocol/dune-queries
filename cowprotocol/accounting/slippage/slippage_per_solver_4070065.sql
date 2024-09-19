-- This query returns slippage per solver over a period of time,
-- evaluated in both usd and the native token of the chain.
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- The columns of the result are
-- - solver_address: address of the solver executing the settlement
-- - slippage_usd: USD value of slippage
-- - slippage_wei: value of slippage in atoms of native token

select
    solver_address,
    sum(slippage_usd) as slippage_usd,
    sum(slippage_wei) as slippage_wei
from "query_4070059(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
group by solver_address
