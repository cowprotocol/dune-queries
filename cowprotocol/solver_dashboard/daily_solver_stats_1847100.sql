-- Aggregate different metrics on settlements depending on the solver
-- Parameters:
--   {{last_n_days}}: int the number of days to look back
--   {{aggregate_by}}: string the time period to aggregate by for each solver
--   {{blockchain}}: string the blockchain to query

select
    date_trunc('{{aggregate_by}}', block_date) as day, --noqa: RF04
    name as solver_name,
    max(block_time) as last_solution,
    count(*) as batches_solved,
    sum(dex_swaps) as dex_swaps,
    sum(num_trades) as num_trades,
    sum(gas_used) as gas_used,
    sum(batch_value) as total_batch_value,
    avg(batch_value) as average_batch_volume,
    avg(num_trades) as average_batch_size,
    1.0 * sum(gas_used) / sum(num_trades) as average_gas_per_trade,
    1.0 * sum(dex_swaps) / sum(num_trades) as average_dex_swaps_per_trade
from cow_protocol_{{blockchain}}.batches
inner join cow_protocol_{{blockchain}}.solvers on solver_address = address
where environment = 'prod' and block_date > now() - interval '{{last_n_days}}' day and active = True
group by name, date_trunc('{{aggregate_by}}', block_date)
order by num_trades desc
