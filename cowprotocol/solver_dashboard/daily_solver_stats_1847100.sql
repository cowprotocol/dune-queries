--aggregate different metrics on settlements depending on the solver
-- Parameters:
--   {{LastNDays}}: int the number of days to look back
--   {{Aggregate by}}: string the time period to aggregate by for each solver
--   {{blockchain}}: string the blockchain to query

select date_trunc('{{Aggregate by}}', block_date) as day,
    name as solver_name,
    -- concat(environment, '-', name) as name,
    max(block_time) last_solution,
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
    join cow_protocol_{{blockchain}}.solvers ON solver_address = address
where environment = 'prod'
    and block_date > now() - interval '{{LastNDays}}' day
    and active = True
group by name,
    date_trunc('{{Aggregate by}}', block_date)
order by num_trades desc