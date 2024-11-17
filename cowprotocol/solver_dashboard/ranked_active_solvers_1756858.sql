-- This query used incompatible data types from Dune SQL alpha and may need to be updated.
-- More details can be found on https://dune.com/docs/query/dunesql-changes/
with solver_info as (
    select
        name as solver_name,
        -- concat(environment, '-', name) as name,
        max(b.block_time) as last_solution,
        count(*) as batches_solved,
        sum(dex_swaps) as dex_swaps,
        sum(num_trades) as num_trades,
        sum(gas_used) as gas_used,
        sum(batch_value) as total_batch_value,
        avg(batch_value) as average_batch_volume,
        avg(num_trades) as average_batch_size,
        sum(surplus_usd) as total_surplus,
        1.0 * sum(gas_used) / sum(num_trades) as average_gas_per_trade,
        1.0 * sum(dex_swaps) / sum(num_trades) as average_dex_swaps_per_trade
    from cow_protocol_{{blockchain}}.batches as b
    inner join cow_protocol_{{blockchain}}.solvers on solver_address = address
    inner join cow_protocol_{{blockchain}}.trades as t
        on b.tx_hash = t.tx_hash
    where environment not in ('test', 'service') and t.block_date > now() - interval '{{last_n_days}}' day and active = True
    group by name
    order by num_trades desc
)

select -- noqa: ST06
    row_number() over (order by average_gas_per_trade) as rk,
    *
from solver_info
order by rk
