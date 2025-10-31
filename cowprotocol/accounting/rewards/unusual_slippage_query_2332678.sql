-- Query that detects high slippage settlements
-- Parameters:
--  {{start_time}} - the start date timestamp for the accounting period  (inclusively)
--  {{end_time}} - the end date timestamp for the accounting period (exclusively)
-- {{min_absolute_slippage_tolerance}} -- the minimum absolute threshold above which a tx is may be flagged as high-slippage
-- {{relative_slippage_tolerance}} -- the minimum relative threshold (wrt batch value) above which a tx may be flagged as high-slippage
--  {{significant_slippage_value}} -- the absolute threshold above which a tx is always flagged as high-slippage
with
results_per_tx as (
    select * from "query_4070065(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}',slippage_table_name='slippage_per_transaction')"
),

url_helper as (
    select
        case
            when '{{blockchain}}' = 'ethereum' then 'eth'
            when '{{blockchain}}' = 'bnb' then 'bsc'
            else '{{blockchain}}'
        end
)

select  --noqa: ST06
    rpt.block_time,
    concat(environment, '-', name) as solver_name,
    concat(
        '<a href="https://dune.com/queries/4059683',
        '?blockchain={{blockchain}}',
        '&start_time={{start_time}}',
        '&end_time={{end_time}}',
        '&slippage_table_name=raw_slippage_breakdown',
        '" target="_blank">link</a>'
    ) as token_breakdown,
    concat(
        '<a href="https://app.blocksec.com/explorer/tx/',
        (select * from url_helper),
        '/0x',
        to_hex(rpt.tx_hash),
        '" target="_blank">0x',
        to_hex(rpt.tx_hash),
        '</a>'
    ) as tx_hash,
    slippage_usd,
    batch_value,
    100 * slippage_usd / batch_value as relative_slippage,
    imbalance_usd,
    protocol_fee_usd,
    network_fee_usd
from results_per_tx as rpt
inner join cow_protocol_{{blockchain}}.batches as b on rpt.tx_hash = b.tx_hash
inner join cow_protocol_{{blockchain}}.solvers on address = rpt.solver_address
where (
    abs(slippage_usd) > {{min_absolute_slippage_tolerance}}
    and
    100.0 * abs(slippage_usd) / batch_value > {{relative_slippage_tolerance}}
) or abs(slippage_usd) > {{significant_slippage_value}}
order by relative_slippage
