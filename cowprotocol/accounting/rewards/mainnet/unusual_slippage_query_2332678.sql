-- Query that detects high slippage settlements
-- Parameters:
--  {{start_time}} - the start date timestamp for the accounting period  (inclusively)
--  {{end_time}} - the end date timestamp for the accounting period (exclusively)
-- {{min_absolute_slippage_tolerance}} -- the minimum absolute threshold above which a tx is may be flagged as high-slippage
-- {{relative_slippage_tolerance}} -- the minimum relative threshold (wrt batch value) above which a tx may be flagged as high-slippage
--  {{significant_slippage_value}} -- the absolute threshold above which a tx is always flagged as high-slippage
with
results_per_tx as (
    select * from "query_3427730(start_time='{{start_time}}',end_time='{{end_time}}',cte_name='results_per_tx')"
)

select  --noqa: ST06
    block_time,
    concat(environment, '-', name) as solver_name,
    concat('<a href="https://dune.com/queries/1955401?TxHash=', cast(rpt.tx_hash as varchar), '&sidebar=none" target="_blank">link</a>') as token_breakdown,
    rpt.tx_hash,
    usd_value,
    batch_value,
    100 * usd_value / batch_value as relative_slippage
from results_per_tx as rpt
inner join cow_protocol_ethereum.batches as b on rpt.tx_hash = b.tx_hash
inner join cow_protocol_ethereum.solvers on address = rpt.solver_address
where (
    abs(usd_value) > {{min_absolute_slippage_tolerance}}
    and
    100.0 * abs(usd_value) / batch_value > {{relative_slippage_tolerance}}
) or abs(usd_value) > {{significant_slippage_value}}
order by relative_slippage
