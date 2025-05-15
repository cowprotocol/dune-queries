-- This query provides various metrics to compare the different operating solvers.
-- It looks back over a period of LastNDays Days
-- Parameters:
--   {{last_n_days}}: int, corresponding to the number of days to look back

with settled_batches as (
    select distinct tx_hash
    from cow_protocol_{{blockchain}}.trades
    where block_date > now() - interval '{{last_n_days}}' day
),

trade_agg as (
    select
        tx_hash,
        count(*) as num_trades,
        sum(surplus_usd) as surplus_usd
    from cow_protocol_{{blockchain}}.trades
    where block_date > now() - interval '{{last_n_days}}' day
    group by tx_hash
),

settled_batch_data as (
    select
        b.tx_hash,
        b.block_time,
        b.solver_address,
        b.batch_value,
        b.gas_used,
        b.dex_swaps
    from cow_protocol_{{blockchain}}.batches as b
    inner join settled_batches as sb on b.tx_hash = sb.tx_hash
),

solver_info as (
    select
        s.name as solver_name,
        max(b.block_time) as last_solution,
        count(*) as batches_solved,
        sum(b.dex_swaps) as dex_swaps,
        sum(t.num_trades) as num_trades,
        sum(b.gas_used) as gas_used,
        sum(b.batch_value) as total_batch_value,
        avg(b.batch_value) as average_batch_volume,
        avg(t.num_trades) as average_batch_size,
        sum(t.surplus_usd) as total_surplus,
        1.0 * sum(b.gas_used) / nullif(sum(t.num_trades), 0) as average_gas_per_trade,
        1.0 * sum(b.dex_swaps) / nulliff(sum(t.num_trades), 0) as average_dex_swaps_per_trade
    from settled_batch_data as b
    inner join cow_protocol_{{blockchain}}.solvers as s
        on b.solver_address = s.address
    inner join trade_agg as t
        on b.tx_hash = t.tx_hash
    where s.environment not in ('test', 'service') and s.active = TRUE
    group by s.name
)

select -- noqa: ST06
    row_number() over (order by average_gas_per_trade) as rk,
    *
from solver_info
order by rk;
