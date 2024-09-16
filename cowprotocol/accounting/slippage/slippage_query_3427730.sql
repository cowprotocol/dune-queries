select
    solver_address,
    concat(environment, '-', name) as solver_name,
    sum(slippage_usd) as usd_value,
    sum(slippage_native_atom) as eth_slippage_wei,
    concat(
        '<a href="https://dune.com/queries/4070059',
        '&blockchain={{blockchain}}',
        '&start_time={{start_time}}',
        '&end_time={{end_time}}',
        '" target="_blank">link</a>'
    ) as slippage_per_transaction
from "query_4070065(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
inner join cow_protocol_{{blockchain}}.solvers
    on solver_address = address
