WITH solver_info as (
    SELECT 
        date_trunc('{{Aggregate by}}', block_date) as day,
        name,
        count(*) as batches_solved,
        sum(dex_swaps) as dex_swaps,
        sum(num_trades) as num_trades,
        sum(gas_used) as gas_used,
        sum(batch_value) as total_batch_value,
        avg(batch_value) as average_batch_volume,
        avg(num_trades) as average_batch_size
    FROM cow_protocol_ethereum.batches
    JOIN cow_protocol_ethereum.solvers 
        ON solver_address = address
    WHERE environment = 'prod'
        AND block_date > cast('2022-03-01' as timestamp)
        -- AND active = True
    GROUP BY 
        name,
        date_trunc('{{Aggregate by}}', block_date)
)
SELECT 
    day,
    name as solver_name,
    batches_solved,
    num_trades,
    dex_swaps,
    1.0 * gas_used / num_trades as average_gas_per_trade,
    1.0 * dex_swaps / num_trades as average_dex_swaps_per_trade,
    average_batch_size,
    total_batch_value,
    average_batch_volume
FROM solver_info
ORDER BY num_trades DESC
