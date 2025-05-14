-- This query provides various metrics to compare the different operating solvers.
-- It looks back over a period of LastNDays Days
-- Parameters:
--   {{last_n_days}}: int the number of days to look back

WITH settled_batches AS (
    SELECT DISTINCT tx_hash
    FROM cow_protocol_{{blockchain}}.trades
    WHERE block_date > now() - interval '{{last_n_days}}' day
),

trade_agg AS (
    SELECT
        tx_hash,
        COUNT(*) AS num_trades,
        SUM(surplus_usd) AS surplus_usd
    FROM cow_protocol_{{blockchain}}.trades
    WHERE block_date > now() - interval '{{last_n_days}}' day
    GROUP BY tx_hash
),

settled_batch_data AS (
    SELECT
        b.tx_hash,
        b.block_time,
        b.solver_address,
        b.batch_value,
        b.gas_used,
        b.dex_swaps
    FROM cow_protocol_{{blockchain}}.batches b
    INNER JOIN settled_batches sb ON b.tx_hash = sb.tx_hash
),

solver_info AS (
    SELECT
        s.name AS solver_name,
        MAX(b.block_time) AS last_solution,
        COUNT(*) AS batches_solved,
        SUM(b.dex_swaps) AS dex_swaps,
        SUM(t.num_trades) AS num_trades,
        SUM(b.gas_used) AS gas_used,
        SUM(b.batch_value) AS total_batch_value,
        AVG(b.batch_value) AS average_batch_volume,
        AVG(t.num_trades) AS average_batch_size,
        SUM(t.surplus_usd) AS total_surplus,
        1.0 * SUM(b.gas_used) / NULLIF(SUM(t.num_trades), 0) AS average_gas_per_trade,
        1.0 * SUM(b.dex_swaps) / NULLIF(SUM(t.num_trades), 0) AS average_dex_swaps_per_trade
    FROM settled_batch_data b
    INNER JOIN cow_protocol_{{blockchain}}.solvers s
        ON b.solver_address = s.address
    INNER JOIN trade_agg t
        ON b.tx_hash = t.tx_hash
    WHERE s.environment NOT IN ('test', 'service') AND s.active = TRUE
    GROUP BY s.name
)

SELECT -- noqa: ST06
    ROW_NUMBER() OVER (ORDER BY average_gas_per_trade) AS rk,
    *
FROM solver_info
ORDER BY rk;
