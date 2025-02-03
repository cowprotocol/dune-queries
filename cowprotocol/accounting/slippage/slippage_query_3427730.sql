select
    solver_address,
    concat(environment, '-', name) as solver_name,
    slippage_usd as usd_value,
    1.0 * slippage_wei as eth_slippage_wei,
    concat(
        '<a href="https://dune.com/queries/4070065',
        '&blockchain=ethereum',
        '&price_feed=dune_price_feed'
        '&start_time={{start_time}}',
        '&end_time={{end_time}}',
        '&slippage_table_name=slippage_per_transaction',
        '" target="_blank">link</a>'
    ) as slippage_per_transaction
from "query_4070065(blockchain='{{blockchain}}',price_feed='dune_price_feed',start_time='{{start_time}}',end_time='{{end_time}}',slippage_table_name='slippage_per_solver')"
inner join cow_protocol_{{blockchain}}.solvers
    on solver_address = address
